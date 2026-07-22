from __future__ import annotations

import asyncio
import base64
import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent))
try:
    import sinomo_adapter
except ModuleNotFoundError:
    sinomo_adapter = None


@unittest.skipIf(sinomo_adapter is None, "tau3 voice dependencies are not installed")
class SinomoAdapterContractTests(unittest.TestCase):
    def tick_result(self):
        from tau2.voice.audio_native.tick_result import TickResult

        return TickResult(
            tick_number=0,
            audio_sent_bytes=1600,
            audio_sent_duration_ms=200,
            user_audio_data=b"",
            cumulative_user_audio_at_tick_start_ms=0,
            bytes_per_tick=1600,
            bytes_per_second=8000,
            silence_byte=b"\xff",
        )

    def test_audio_transcript_and_tool_events_map_to_tau_tick_result(self):
        adapter = sinomo_adapter.SinomoRealtimeAdapter(200, "/missing/bridge")
        result = self.tick_result()
        pcm_silence = b"\x00\x00" * 480

        asyncio.run(
            adapter._process_event(
                result,
                {
                    "type": "output_audio_delta",
                    "audio_base64": base64.b64encode(pcm_silence).decode(),
                    "item_id": "item_1",
                },
            )
        )
        asyncio.run(
            adapter._process_event(
                result,
                {
                    "type": "output_transcript_delta",
                    "delta": "Hello",
                    "item_id": "item_1",
                },
            )
        )
        asyncio.run(
            adapter._process_event(
                result,
                {
                    "type": "tool_call_completed",
                    "call_id": "call_1",
                    "name": "lookup_order",
                    "arguments": '{"order_id":"A-42"}',
                },
            )
        )

        self.assertEqual(result.agent_audio_chunks[0][1], "item_1")
        self.assertGreater(len(result.agent_audio_chunks[0][0]), 0)
        self.assertEqual(
            adapter._utterance_transcripts["item_1"].transcript_received, "Hello"
        )
        self.assertEqual(result.tool_calls[0].id, "call_1")
        self.assertEqual(result.tool_calls[0].arguments, {"order_id": "A-42"})


if __name__ == "__main__":
    unittest.main()
