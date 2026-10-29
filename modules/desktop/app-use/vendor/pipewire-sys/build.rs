// Copyright The pipewire-rs Contributors. SPDX-License-Identifier: MIT
// Sinomo: compile against bundled headers and resolve optional runtime symbols.
use quote::{format_ident, quote};
use std::{env, path::PathBuf};
mod dependency_headers;

#[derive(Default)]
struct RequiredSymbols(std::collections::BTreeSet<String>);
impl<'ast> syn::visit::Visit<'ast> for RequiredSymbols {
    fn visit_path(&mut self, path: &'ast syn::Path) {
        if path.segments.len() == 2 && path.segments[0].ident == "pw_sys" {
            self.0.insert(path.segments[1].ident.to_string());
        }
        syn::visit::visit_path(self, path);
    }
}
fn required_symbols() -> RequiredSymbols {
    use syn::visit::Visit;
    let root = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap()).join("../pipewire/src");
    println!("cargo:rerun-if-changed={}", root.display());
    let mut pending = vec![root];
    let mut required = RequiredSymbols::default();
    while let Some(path) = pending.pop() {
        if path.is_dir() {
            pending.extend(
                std::fs::read_dir(path)
                    .unwrap()
                    .map(|entry| entry.unwrap().path()),
            );
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            required.visit_file(&syn::parse_file(&std::fs::read_to_string(path).unwrap()).unwrap());
        }
    }
    required
}
fn main() {
    println!("cargo:rerun-if-changed=wrapper.h");
    println!("cargo:rerun-if-changed=dependency_headers.rs");
    let out = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    let mut builder = bindgen::Builder::default()
        .header("wrapper.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .size_t_is_usize(true)
        .allowlist_function("pw_.*")
        .allowlist_type("pw_.*")
        .allowlist_var("pw_.*")
        .allowlist_var("PW_.*")
        .blocklist_function("spa_.*")
        .blocklist_type("spa_.*")
        .blocklist_item("spa_.*")
        .raw_line("use spa_sys::*;")
        .clang_macro_fallback()
        .clang_macro_fallback_build_dir(&out);
    for path in dependency_headers::include_paths() {
        builder = builder.clang_arg(format!("-I{}", path.display()));
    }
    let raw = builder.generate().expect("PipeWire headers").to_string();
    let file = syn::parse_file(&raw).expect("generated bindings");
    let required = required_symbols();
    let mut retained = Vec::new();
    let mut fields = Vec::new();
    let mut loads = Vec::new();
    let mut wrappers = Vec::new();
    for item in file.items {
        let syn::Item::ForeignMod(foreign) = item else {
            retained.push(item);
            continue;
        };
        for item in foreign.items {
            match item {
                syn::ForeignItem::Fn(function) => {
                    let name = &function.sig.ident;
                    let inputs = &function.sig.inputs;
                    let output = &function.sig.output;
                    let variadic = &function.sig.variadic;
                    let ty = quote!(unsafe extern "C" fn(#inputs #variadic) #output);
                    let bytes = syn::LitByteStr::new(format!("{name}\0").as_bytes(), name.span());
                    if !required.0.contains(&name.to_string()) {
                        continue;
                    }
                    fields.push(quote!(pub(crate) #name: #ty));
                    loads.push(quote!(#name: *unsafe { library.get::<#ty>(#bytes) }
                        .map_err(|error| format!("required PipeWire symbol {}: {}", stringify!(#name), error))?));
                    let attrs = &function.attrs;
                    if variadic.is_some() {
                        // Rust cannot define variadic trampolines on stable. Return the
                        // checked function pointer; private callers retain C varargs ABI.
                        wrappers.push(quote!(#(#attrs)* pub unsafe fn #name() -> #ty {
                            crate::runtime::symbol(|api| api.#name)
                        }));
                    } else {
                        let arguments = inputs.iter().map(|a| match a {
                            syn::FnArg::Typed(t) => &t.pat,
                            _ => panic!("C function receiver"),
                        });
                        wrappers.push(
                            quote!(#(#attrs)* pub unsafe extern "C" fn #name(#inputs) #output {
                                let function = crate::runtime::symbol(|api| api.#name);
                                unsafe { function(#(#arguments),*) }
                            }),
                        );
                    }
                }
                syn::ForeignItem::Static(value) => {
                    let name = &value.ident;
                    let ty = &value.ty;
                    let bytes = syn::LitByteStr::new(format!("{name}\0").as_bytes(), name.span());
                    if !required.0.contains(&name.to_string()) {
                        continue;
                    }
                    fields.push(quote!(pub(crate) #name: *mut #ty));
                    loads.push(quote!(#name: *unsafe { library.get::<*mut #ty>(#bytes) }
                        .map_err(|error| format!("required PipeWire data symbol {}: {}", stringify!(#name), error))?));
                    wrappers.push(quote!(pub unsafe fn #name() -> *mut #ty {
                        crate::runtime::symbol(|api| api.#name)
                    }));
                }
                _ => panic!("unsupported foreign binding"),
            }
        }
    }
    let api = format_ident!("OptionalPipeWireApi");
    let generated = quote! {
        #(#retained)*
        pub(crate) struct #api {
            #(#fields,)*
            _library: libloading::Library,
        }
        // The table only stores immutable symbol addresses. Calling the C API
        // remains unsafe; its existing object/thread ownership rules still apply.
        unsafe impl Send for #api {}
        unsafe impl Sync for #api {}
        impl #api {
            pub(crate) unsafe fn load(name: &std::ffi::OsStr) -> Result<Self, String> {
                let library = unsafe { libloading::Library::new(name) }.map_err(|e| e.to_string())?;
                Ok(Self { #(#loads,)* _library: library })
            }
        }
        #(#wrappers)*
    };
    std::fs::write(out.join("bindings.rs"), generated.to_string()).unwrap();
}
