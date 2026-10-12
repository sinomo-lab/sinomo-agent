use super::*;

fn file_system_id() -> DfsFileSystemId {
    DfsFileSystemId::from_legacy_named("sifs").expect("valid filesystem id")
}

fn object_id() -> DfsObjectId {
    DfsObjectId::from_legacy_named("obj_readme").expect("valid object id")
}

mod prepared_operation;
