//! Local Android UI adapter for the existing node permission store. This is
//! registered only in the embedded app process; no RPC/tool can invoke it.
#[cfg(target_os = "android")]
use jni::objects::{JClass, JObject, JString};
#[cfg(target_os = "android")]
use jni::refs::Global;
#[cfg(target_os = "android")]
use jni::{NativeMethod, jni_sig, jni_str};

#[cfg(target_os = "android")]
pub(crate) fn install(app: &sui::AndroidApp) -> jni::errors::Result<()> {
    let vm = unsafe { jni::JavaVM::from_raw(app.vm_as_ptr().cast()) };
    let raw_activity = app.activity_as_ptr() as jni::sys::jobject;
    vm.attach_current_thread(|env| {
        let activity = unsafe { env.as_cast_raw::<Global<JObject>>(&raw_activity)? };
        let loader = env
            .call_method(
                &activity,
                jni_str!("getClassLoader"),
                jni_sig!("()Ljava/lang/ClassLoader;"),
                &[],
            )?
            .l()?;
        let name = env.new_string("net.sinomo.app.use.AppUseConsentActivity")?;
        let result = env.call_method(
            &loader,
            jni_str!("loadClass"),
            jni_sig!("(Ljava/lang/String;)Ljava/lang/Class;"),
            &[(&name).into()],
        );
        let class = match result {
            Ok(value) => value.l()?,
            Err(error) => {
                // A plain cargo-apk build has no service classes. Keep that
                // optional capability unavailable without poisoning the UI thread.
                env.exception_clear();
                return Err(error);
            }
        };
        let class = JClass::cast_local(env, class)?;
        unsafe {
            env.register_native_methods(
                &class,
                &[
                    NativeMethod::from_raw_parts(
                        jni_str!("grantDisplay"),
                        jni_str!("(Ljava/lang/String;Z)Z"),
                        grant_display as *mut _,
                    ),
                    NativeMethod::from_raw_parts(
                        jni_str!("revokeAccess"),
                        jni_str!("(Ljava/lang/String;)Z"),
                        revoke_access as *mut _,
                    ),
                ],
            )?;
        }
        Ok(())
    })
}

#[cfg(target_os = "android")]
extern "system" fn grant_display<'local>(
    mut env: jni::EnvUnowned<'local>,
    _class: JClass<'local>,
    resource: JString<'local>,
    input: jni::sys::jboolean,
) -> jni::sys::jboolean {
    env.with_env(|env| -> jni::errors::Result<_> {
        let resource = resource.try_to_string(env)?;
        let Ok(data_dir) = sinomo_runtime::local::SinomoDataDir::from_env_or_home() else {
            return Ok(false);
        };
        let store = sinomo_runtime::node::LocalNodePermissionStore::new(data_dir.root());
        Ok(grant_local_display(&store, &resource, input))
    })
    .resolve::<jni::errors::ThrowRuntimeExAndDefault>()
}

#[cfg(target_os = "android")]
extern "system" fn revoke_access<'local>(
    mut env: jni::EnvUnowned<'local>,
    _class: JClass<'local>,
    resource: JString<'local>,
) -> jni::sys::jboolean {
    env.with_env(|env| -> jni::errors::Result<_> {
        let resource = resource.try_to_string(env)?;
        let Ok(data_dir) = sinomo_runtime::local::SinomoDataDir::from_env_or_home() else {
            return Ok(false);
        };
        let store = sinomo_runtime::node::LocalNodePermissionStore::new(data_dir.root());
        Ok(revoke_local_access(&store, &resource))
    })
    .resolve::<jni::errors::ThrowRuntimeExAndDefault>()
}
fn revoke_local_access(
    store: &sinomo_runtime::node::LocalNodePermissionStore,
    resource: &str,
) -> bool {
    if resource != "display:android:accessibility:display:0" {
        return false;
    }
    store
        .mutate(|permissions| {
            let capture = permissions.hide_resource_operation(resource, "capture")?;
            let input = permissions.hide_resource_operation(resource, "input")?;
            Ok(capture || input)
        })
        .is_ok()
}

fn grant_local_display(
    store: &sinomo_runtime::node::LocalNodePermissionStore,
    resource: &str,
    input: bool,
) -> bool {
    let accessibility = resource == "display:android:accessibility:display:0";
    if (!accessibility && !resource.starts_with("display:android:display:0:"))
        || resource.len() > 128
    {
        return false;
    }
    let result = store.mutate(|permissions| {
        // The Settings control owns broader consent; sharing cannot enable it.
        if !permissions.remote_management_enabled() {
            return Ok(false);
        }
        let mut changed = permissions.expose_resource_operation(resource, "capture")?;
        if !accessibility {
            changed |= permissions.expose_resource_operation(resource, "stop")?;
        }
        if input {
            changed |= permissions.expose_resource_operation(resource, "input")?;
        } else {
            changed |= permissions.hide_resource_operation(resource, "input")?;
        }
        Ok(changed)
    });
    result.is_ok_and(|(permissions, _)| permissions.remote_management_enabled())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::node::LocalNodePermissionStore;
    #[test]
    fn persistent_accessibility_grants_are_independent_and_revocable() {
        let root = std::env::temp_dir().join(format!(
            "app-use-accessibility-{}",
            sinomo_runtime::core::Id::generate()
        ));
        let store = LocalNodePermissionStore::new(&root);
        let resource = "display:android:accessibility:display:0";
        assert!(!grant_local_display(&store, resource, true));
        store
            .mutate(|permissions| Ok(permissions.set_remote_management_enabled(true)))
            .unwrap();
        assert!(grant_local_display(&store, resource, true));
        assert!(store.resource_operation_exposed(resource, "capture"));
        assert!(store.resource_operation_exposed(resource, "input"));
        assert!(!store.resource_operation_exposed(resource, "stop"));
        assert!(revoke_local_access(&store, resource));
        assert!(!store.resource_operation_exposed(resource, "capture"));
        assert!(!store.resource_operation_exposed(resource, "input"));
        assert!(store.load().unwrap().remote_management_enabled());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn display_grants_require_local_consent_and_input_is_explicit() {
        let root = std::env::temp_dir().join(format!(
            "app-use-consent-{}",
            sinomo_runtime::core::Id::generate()
        ));
        let store = LocalNodePermissionStore::new(&root);
        let resource = "display:android:display:0:123:1080x1920";
        assert!(!grant_local_display(&store, resource, true));
        assert!(!store.load().unwrap().remote_management_enabled());
        store
            .mutate(|p| Ok(p.set_remote_management_enabled(true)))
            .unwrap();
        assert!(!grant_local_display(&store, "display:another-device", true));
        assert!(grant_local_display(&store, resource, false));
        assert!(
            !store
                .load()
                .unwrap()
                .resource_operation_exposed(resource, "input")
        );
        assert!(grant_local_display(&store, resource, true));
        assert!(
            store
                .load()
                .unwrap()
                .resource_operation_exposed(resource, "input")
        );
        assert!(grant_local_display(&store, resource, false));
        let permissions = store.load().unwrap();
        assert!(!permissions.resource_operation_exposed(resource, "input"));
        assert!(permissions.resource_operation_exposed(resource, "capture"));
        std::fs::remove_dir_all(root).unwrap();
    }
}
