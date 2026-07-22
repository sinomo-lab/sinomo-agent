"""tau3-bench discrete-time adapter backed by Sinomo realtime audio.

This module is loaded from a separately checked out, revision-pinned tau3-bench
environment. It intentionally does not vendor or import tau3-bench in Sinomo.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import subprocess
import threading
from pathlib import Path
from typing import Any

from loguru import logger
from pydantic import BaseModel, ConfigDict

from tau2.agent.discrete_time_audio_native_agent import DiscreteTimeAudioNativeAgent
from tau2.config import DEFAULT_AUDIO_NATIVE_TICK_TIMEOUT_BUFFER
from tau2.data_model.message import ToolCall
from tau2.data_model.simulation import VoiceRunConfig
from tau2.environment.tool import Tool
from tau2.voice.audio_native.adapter import DiscreteTimeAdapter
from tau2.voice.audio_native.async_loop import BackgroundAsyncLoop
from tau2.voice.audio_native.audio_converter import StreamingTelephonyConverter
from tau2.voice.audio_native.tick_result import TickResult, UtteranceTranscript


SINOMO_AGENT_FACTORY = "sinomo_realtime_audio_agent"


class BridgeProtocolError(RuntimeError):
    """The bridge exited, returned invalid JSON, or rejected a command."""


class _BridgeEvent(BaseModel):
    model_config = ConfigDict(extra="allow")

    type: str


class _BridgeClient:
    def __init__(self, binary: str | Path):
        self.binary = Path(binary).expanduser().resolve()
        self._process: subprocess.Popen[str] | None = None
        self._lock = threading.Lock()

    @property
    def is_connected(self) -> bool:
        return self._process is not None and self._process.poll() is None

    def start(self) -> None:
        if self.is_connected:
            return
        if not self.binary.is_file():
            raise BridgeProtocolError(f"Sinomo tau3 bridge not found: {self.binary}")
        self._process = subprocess.Popen(
            [str(self.binary)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

    def request(self, command: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            process = self._process
            if process is None or process.poll() is not None:
                raise BridgeProtocolError("Sinomo tau3 bridge is not running")
            assert process.stdin is not None
            assert process.stdout is not None
            process.stdin.write(json.dumps(command, separators=(",", ":")) + "\n")
            process.stdin.flush()
            line = process.stdout.readline()
            if not line:
                code = process.poll()
                raise BridgeProtocolError(
                    f"Sinomo tau3 bridge closed its output (exit={code})"
                )
            try:
                response = json.loads(line)
            except json.JSONDecodeError as error:
                raise BridgeProtocolError(
                    f"Sinomo tau3 bridge returned invalid JSON: {error}"
                ) from error
            if not response.get("ok"):
                raise BridgeProtocolError(response.get("error", "bridge command failed"))
            return response

    def close(self) -> None:
        process = self._process
        if process is None:
            return
        if process.poll() is None:
            try:
                self.request({"command": "close"})
            except (BridgeProtocolError, BrokenPipeError):
                process.terminate()
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)
        self._process = None


def _tau_tools(tools: list[Tool]) -> list[dict[str, Any]]:
    definitions = []
    for tool in tools:
        schema = tool.openai_schema
        function = schema.get("function", schema)
        definitions.append(
            {
                "name": function["name"],
                "description": function.get("description"),
                "parameters": function.get("parameters", {"type": "object"}),
            }
        )
    return definitions


class SinomoRealtimeAdapter(DiscreteTimeAdapter):
    """Translate tau3's 8 kHz mu-law ticks to Sinomo's PCM realtime API."""

    def __init__(
        self,
        tick_duration_ms: int,
        bridge_binary: str | Path,
        provider: str = "openai",
        model: str = "gpt-realtime-2",
        endpoint: str | None = None,
        voice: str | None = None,
        send_audio_instant: bool = False,
    ) -> None:
        super().__init__(
            tick_duration_ms=tick_duration_ms,
            send_audio_instant=send_audio_instant,
        )
        if provider != "openai":
            raise ValueError(
                "the initial tau3 voice adapter supports provider=openai only"
            )
        self.provider = provider
        self.model = model
        self.endpoint = endpoint
        self.voice = voice
        self._bridge = _BridgeClient(bridge_binary)
        self._converter = StreamingTelephonyConverter(
            input_sample_rate=24_000,
            output_sample_rate=24_000,
        )
        self._bg_loop = BackgroundAsyncLoop()
        self._connected = False
        self._pcm_bytes_per_packet = 24_000 * 2 * self._voip_interval_ms // 1000

    @property
    def is_connected(self) -> bool:
        return self._connected and self._bridge.is_connected and self._bg_loop.is_running

    def connect(
        self,
        system_prompt: str,
        tools: list[Tool],
        vad_config: Any = None,
        modality: str = "audio",
    ) -> None:
        if modality != "audio":
            raise ValueError("Sinomo tau3 adapter currently supports audio modality only")
        if self._connected:
            self.disconnect()
        if vad_config is not None:
            mode = getattr(getattr(vad_config, "mode", None), "value", None)
            if mode not in (None, "server_vad"):
                raise ValueError("Sinomo tau3 adapter currently requires server_vad")

        self._bridge.start()
        self._bg_loop.start()
        try:
            self._bridge.request(
                {
                    "command": "connect",
                    "provider": self.provider,
                    "model": self.model,
                    "instructions": system_prompt,
                    "endpoint": self.endpoint,
                    "voice": self.voice,
                    "turn_detection": "server-vad",
                    "tools": _tau_tools(tools),
                }
            )
            self._connected = True
        except Exception:
            self._bg_loop.stop()
            self._bridge.close()
            raise

    def disconnect(self) -> None:
        if not self._connected and not self._bridge.is_connected:
            return
        try:
            self._bridge.close()
        finally:
            self._bg_loop.stop()
            self._connected = False
            self._tick_count = 0
            self._cumulative_user_audio_ms = 0
            self.clear_buffers()
            self._converter.reset()

    def run_tick(
        self, user_audio: bytes, tick_number: int | None = None
    ) -> TickResult:
        if not self.is_connected:
            raise RuntimeError("Sinomo tau3 adapter is not connected")
        if tick_number is None:
            tick_number = self._tick_count
        self._tick_count = tick_number + 1
        return self._bg_loop.run_coroutine(
            self._async_run_tick(user_audio, tick_number),
            timeout=(
                self.tick_duration_ms / 1000
                + DEFAULT_AUDIO_NATIVE_TICK_TIMEOUT_BUFFER
            ),
        )

    async def _flush_pending_tool_results(self) -> None:
        for call_id, result, request_response, _is_error in self._pending_tool_results:
            await asyncio.to_thread(
                self._bridge.request,
                {
                    "command": "tool_result",
                    "call_id": call_id,
                    "output": result,
                    "request_response": request_response,
                },
            )
        self._pending_tool_results.clear()

    async def _execute_tick(
        self,
        user_audio: bytes,
        tick_number: int,
        result: TickResult,
        tick_start: float,
    ) -> None:
        pcm = self._converter.convert_input(user_audio)
        if self.send_audio_instant:
            events = await self._tick_request(
                tick_number,
                pcm,
                self._remaining_ms(tick_start),
            )
            for event in events:
                await self._process_event(result, event)
            return

        chunks = [
            pcm[offset : offset + self._pcm_bytes_per_packet]
            for offset in range(0, len(pcm), self._pcm_bytes_per_packet)
        ] or [b""]
        for chunk in chunks:
            events = await self._tick_request(
                tick_number,
                chunk,
                min(self._voip_interval_ms, self._remaining_ms(tick_start)),
            )
            for event in events:
                await self._process_event(result, event)

    def _remaining_ms(self, tick_start: float) -> int:
        elapsed_ms = (asyncio.get_running_loop().time() - tick_start) * 1000
        return max(0, int(self.tick_duration_ms - elapsed_ms))

    async def _tick_request(
        self, tick_number: int, pcm: bytes, receive_ms: int
    ) -> list[dict[str, Any]]:
        response = await asyncio.to_thread(
            self._bridge.request,
            {
                "command": "tick",
                "tick": tick_number,
                "audio_base64": base64.b64encode(pcm).decode("ascii"),
                "receive_ms": receive_ms,
            },
        )
        return response.get("events", [])

    async def _process_event(
        self, result: TickResult, payload: dict[str, Any]
    ) -> None:
        event_type = payload.get("type", "unknown")
        result.events.append(_BridgeEvent.model_validate(payload))

        if event_type == "output_audio_delta":
            item_id = payload.get("item_id")
            decoded = self._converter.convert_output(
                base64.b64decode(payload.get("audio_base64", ""))
            )
            if result.skip_item_id is not None:
                if item_id == result.skip_item_id:
                    result.truncated_audio_bytes += len(decoded)
                    return
                result.skip_item_id = None
            result.agent_audio_chunks.append((decoded, item_id))
            if item_id:
                transcript = self._utterance_transcripts.setdefault(
                    item_id, UtteranceTranscript(item_id=item_id)
                )
                transcript.add_audio(len(decoded))
            return

        if event_type == "output_transcript_delta":
            item_id = payload.get("item_id")
            delta = payload.get("delta", "")
            if item_id and delta:
                transcript = self._utterance_transcripts.setdefault(
                    item_id, UtteranceTranscript(item_id=item_id)
                )
                transcript.add_transcript(delta)
            return

        if event_type == "input_speech_started":
            result.vad_events.append("speech_started")
            await self._handle_interruption(result, payload.get("audio_start_ms") or 0)
            return

        if event_type == "input_speech_stopped":
            result.vad_events.append("speech_stopped")
            return

        if event_type == "tool_call_completed":
            call_id = payload.get("call_id")
            name = payload.get("name")
            if call_id and name:
                try:
                    arguments = json.loads(payload.get("arguments") or "{}")
                except json.JSONDecodeError:
                    arguments = {}
                result.tool_calls.append(
                    ToolCall(id=call_id, name=name, arguments=arguments)
                )
            return

        if event_type == "provider_error":
            raise BridgeProtocolError(
                f"provider error {payload.get('code')}: {payload.get('message')}"
            )

        logger.debug(f"Sinomo realtime event {event_type} received")

    async def _handle_interruption(
        self, result: TickResult, audio_start_ms: int
    ) -> None:
        has_agent_audio = result.agent_audio_chunks or self._buffered_agent_audio
        if not has_agent_audio:
            return
        last_item_id = (
            result.agent_audio_chunks[-1][1]
            if result.agent_audio_chunks
            else self._buffered_agent_audio[-1][1]
        )
        if self._buffered_agent_audio:
            result.truncated_audio_bytes += sum(
                len(chunk) for chunk, _item_id in self._buffered_agent_audio
            )
            self._buffered_agent_audio.clear()
        result.truncate_agent_audio(
            item_id=last_item_id,
            audio_start_ms=audio_start_ms,
            cumulative_user_audio_at_tick_start_ms=(
                result.cumulative_user_audio_at_tick_start_ms
            ),
            bytes_per_tick=result.bytes_per_tick,
        )
        self._converter.reset()
        if last_item_id is not None:
            played = result.get_played_agent_audio()
            audio_end_ms = int(
                len(played) / self.audio_format.bytes_per_second * 1000
            )
            await asyncio.to_thread(
                self._bridge.request,
                {
                    "command": "truncate",
                    "item_id": last_item_id,
                    "content_index": 0,
                    "audio_end_ms": audio_end_ms,
                },
            )


def create_sinomo_realtime_audio_agent(tools, domain_policy, **kwargs):
    """tau3 agent factory installed over its pinned audio-native factory key."""

    config = kwargs.get("audio_native_config")
    if config is None:
        raise ValueError("tau3 VoiceRunConfig did not provide audio_native_config")
    bridge_binary = os.environ.get("SINOMO_TAU3_BRIDGE_BIN")
    if not bridge_binary:
        raise ValueError("SINOMO_TAU3_BRIDGE_BIN is required")
    adapter = SinomoRealtimeAdapter(
        tick_duration_ms=int(config.tick_duration_ms),
        bridge_binary=bridge_binary,
        provider=config.provider,
        model=config.model,
        endpoint=os.environ.get("SINOMO_TAU3_REALTIME_ENDPOINT"),
        voice=os.environ.get("SINOMO_TAU3_REALTIME_VOICE"),
        send_audio_instant=config.send_audio_instant,
    )
    return DiscreteTimeAudioNativeAgent(
        tools=tools,
        domain_policy=domain_policy,
        tick_duration_ms=int(config.tick_duration_ms),
        modality="audio",
        adapter=adapter,
        send_audio_instant=config.send_audio_instant,
        provider="openai",
        model=config.model,
        reasoning_effort=config.reasoning_effort,
        use_xml_prompt=config.use_xml_prompt,
        audio_taps_dir=kwargs.get("audio_taps_dir"),
    )


class SinomoVoiceRunConfig(VoiceRunConfig):
    """Voice config that selects Sinomo's registered tau3 agent factory."""

    @property
    def effective_agent(self) -> str:
        return SINOMO_AGENT_FACTORY


def install_tau3_agent_factory() -> None:
    """Register the Sinomo adapter in the pinned tau3 registry."""

    from tau2.registry import registry

    existing = registry.get_agent_factory(SINOMO_AGENT_FACTORY)
    if existing is create_sinomo_realtime_audio_agent:
        return
    if existing is not None:
        raise RuntimeError(f"tau3 agent factory already exists: {SINOMO_AGENT_FACTORY}")
    registry.register_agent_factory(
        create_sinomo_realtime_audio_agent,
        SINOMO_AGENT_FACTORY,
    )
