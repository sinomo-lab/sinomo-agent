use std::ptr::NonNull;

use wasmer::{
    MemoryError, MemoryStyle, MemoryType, Pages, TableStyle, TableType,
    sys::{
        BaseTunables, Target, Tunables,
        vm::{VMConfig, VMMemory, VMMemoryDefinition, VMTable, VMTableDefinition},
    },
};

#[derive(Clone)]
pub(super) struct SandboxWasixTunables {
    base: BaseTunables,
    memory_limit_pages: Pages,
}

impl SandboxWasixTunables {
    pub(super) fn for_target(target: &Target, memory_limit_bytes: usize) -> Self {
        let base = BaseTunables::for_target(target);
        let limit_pages = ((memory_limit_bytes as u128).saturating_add(65535) / 65536)
            .min(u128::from(u32::MAX)) as u32;
        let memory_limit_pages = Pages(limit_pages);
        Self {
            base,
            memory_limit_pages,
        }
    }

    fn validate_memory_type(&self, ty: &MemoryType) -> Result<(), MemoryError> {
        if ty.minimum > self.memory_limit_pages
            || ty
                .maximum
                .is_none_or(|maximum| maximum > self.memory_limit_pages)
        {
            return Err(MemoryError::InvalidMemory {
                reason: format!(
                    "sandbox WASIX memory budget exceeded: memory type {ty} is above {} pages",
                    self.memory_limit_pages.0
                ),
            });
        }
        Ok(())
    }
}

impl Tunables for SandboxWasixTunables {
    fn memory_style(&self, memory: &MemoryType) -> MemoryStyle {
        let mut style = self.base.memory_style(memory);
        if let MemoryStyle::Static { bound, .. } = &mut style {
            *bound = (*bound).min(self.memory_limit_pages);
        }
        style
    }

    fn table_style(&self, table: &TableType) -> TableStyle {
        self.base.table_style(table)
    }

    fn create_host_memory(
        &self,
        ty: &MemoryType,
        style: &MemoryStyle,
    ) -> Result<VMMemory, MemoryError> {
        self.validate_memory_type(ty)?;
        self.base.create_host_memory(ty, style)
    }

    unsafe fn create_vm_memory(
        &self,
        ty: &MemoryType,
        style: &MemoryStyle,
        vm_definition_location: NonNull<VMMemoryDefinition>,
    ) -> Result<VMMemory, MemoryError> {
        self.validate_memory_type(ty)?;
        unsafe {
            self.base
                .create_vm_memory(ty, style, vm_definition_location)
        }
    }

    fn create_host_table(&self, ty: &TableType, style: &TableStyle) -> Result<VMTable, String> {
        self.base.create_host_table(ty, style)
    }

    unsafe fn create_vm_table(
        &self,
        ty: &TableType,
        style: &TableStyle,
        vm_definition_location: NonNull<VMTableDefinition>,
    ) -> Result<VMTable, String> {
        unsafe { self.base.create_vm_table(ty, style, vm_definition_location) }
    }

    fn vmconfig(&self) -> &VMConfig {
        self.base.vmconfig()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasmer::sys::Target;

    #[test]
    fn sandbox_wasix_tunables_reject_memory_above_budget() {
        let tunables = SandboxWasixTunables::for_target(&Target::default(), 64 * 1024);

        assert!(
            tunables
                .create_host_memory(
                    &MemoryType::new(1_u32, Some(1_u32), false),
                    &tunables.memory_style(&MemoryType::new(1_u32, Some(1_u32), false)),
                )
                .is_ok()
        );
        assert!(
            tunables
                .create_host_memory(
                    &MemoryType::new(1_u32, Some(2_u32), false),
                    &tunables.memory_style(&MemoryType::new(1_u32, Some(2_u32), false)),
                )
                .is_err()
        );
        assert!(
            tunables
                .create_host_memory(
                    &MemoryType::new(1_u32, None::<u32>, false),
                    &tunables.memory_style(&MemoryType::new(1_u32, None::<u32>, false)),
                )
                .is_err()
        );
    }
}
