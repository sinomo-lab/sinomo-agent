// Copyright The pipewire-rs Contributors.
// SPDX-License-Identifier: MIT

#[allow(non_upper_case_globals)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[allow(unpredictable_function_pointer_comparisons)]
#[allow(clippy::all)]
/// bindgen-generated definitions
mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}
pub use bindings::*;

// pipewire-rs exposes this later enum value unconditionally. Naming the stable
// wire discriminant does not require a newer struct layout, C symbol or codec.
// SPA added Opus immediately after APE in 0.3.68; runtime support is negotiated.
#[allow(non_upper_case_globals)]
pub const SPA_MEDIA_SUBTYPE_opus: spa_media_subtype = 0x10010;

// Manually defined symbols that are manually compiled into a C object file, as they need to be present at link-time.
//
// As SPA is a header-only library, global variables and functions are `static` / `static inline`
// and we need to compile them into a C object ourselves.
//
// For functions, this is handled by bindgens "wrap_static_fns" feature.
//
// The rest is added in modules here.
mod type_info;
pub use type_info::*;

mod command;
pub use command::*;
mod meta;
pub use meta::*;

mod node_command;
pub use node_command::*;
