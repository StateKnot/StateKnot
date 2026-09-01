// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Frozen exact-version model and tool provider bindings.

use std::{collections::BTreeMap, fmt, sync::Arc};

use stateknot_core::{CapabilityIdentity, ErasedTool, Model, ModelDescriptor, ToolDescriptor};
use thiserror::Error;

struct ModelProviderBinding {
    descriptor: ModelDescriptor,
    provider: Arc<dyn Model>,
}

/// Startup-only builder for immutable object-safe model provider bindings.
#[derive(Default)]
pub struct ModelProviderRegistryBuilder {
    bindings: BTreeMap<CapabilityIdentity, ModelProviderBinding>,
}

impl ModelProviderRegistryBuilder {
    /// Maximum exact model revisions installed in one process snapshot.
    pub const MAX_BINDINGS: usize = 4096;

    /// Creates an empty model registry builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one exact provider binding and freezes its descriptor snapshot.
    ///
    /// # Errors
    ///
    /// Rejects capacity exhaustion or a repeated owner/name/version identity.
    pub fn register(&mut self, provider: Arc<dyn Model>) -> Result<(), ModelProviderRegistryError> {
        if self.bindings.len() == Self::MAX_BINDINGS {
            return Err(ModelProviderRegistryError::TooManyBindings);
        }
        let descriptor = provider.descriptor().clone();
        let identity = descriptor.metadata().identity().clone();
        if self.bindings.contains_key(&identity) {
            return Err(ModelProviderRegistryError::DuplicateIdentity {
                identity: Box::new(identity),
            });
        }
        self.bindings.insert(
            identity,
            ModelProviderBinding {
                descriptor,
                provider,
            },
        );
        Ok(())
    }

    /// Freezes this startup snapshot. An empty registry is valid for tool-only workers.
    #[must_use]
    pub fn build(self) -> ModelProviderRegistry {
        ModelProviderRegistry {
            bindings: Arc::new(self.bindings),
        }
    }
}

impl fmt::Debug for ModelProviderRegistryBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelProviderRegistryBuilder")
            .field("bindings", &self.bindings.len())
            .finish_non_exhaustive()
    }
}

/// Immutable exact-version model provider registry.
///
/// Lookup starts from the durable descriptor identity, then compares the full
/// durable descriptor against both the startup snapshot and the provider's
/// current descriptor reference. Provider aliases, model-family names, and
/// fallback selection never participate in this recovery boundary.
#[derive(Clone)]
pub struct ModelProviderRegistry {
    bindings: Arc<BTreeMap<CapabilityIdentity, ModelProviderBinding>>,
}

impl ModelProviderRegistry {
    /// Resolves and revalidates the exact durable model descriptor.
    ///
    /// # Errors
    ///
    /// Returns a missing-binding error or fails closed if any descriptor field
    /// differs from the immutable startup snapshot.
    pub fn resolve(
        &self,
        descriptor: &ModelDescriptor,
    ) -> Result<Arc<dyn Model>, ModelProviderRegistryError> {
        let identity = descriptor.metadata().identity();
        let binding = self.bindings.get(identity).ok_or_else(|| {
            ModelProviderRegistryError::MissingBinding {
                identity: Box::new(identity.clone()),
            }
        })?;
        if &binding.descriptor != descriptor || binding.provider.descriptor() != descriptor {
            return Err(ModelProviderRegistryError::DescriptorMismatch {
                identity: Box::new(identity.clone()),
            });
        }
        Ok(Arc::clone(&binding.provider))
    }

    /// Returns the number of exact model revisions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.bindings.len()
    }

    /// Returns whether this worker has no model bindings.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }
}

impl fmt::Debug for ModelProviderRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelProviderRegistry")
            .field("bindings", &self.bindings.len())
            .finish_non_exhaustive()
    }
}

/// Startup or exact-resolution failure for model providers.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelProviderRegistryError {
    /// The immutable process binding ceiling was reached.
    #[error("model provider registry contains too many bindings")]
    TooManyBindings,
    /// One exact owner/name/version identity was registered twice.
    #[error("model provider identity was registered more than once")]
    DuplicateIdentity {
        /// Repeated exact binding identity.
        identity: Box<CapabilityIdentity>,
    },
    /// No provider implementation owns the durable identity.
    #[error("durable model descriptor has no installed provider binding")]
    MissingBinding {
        /// Missing exact binding identity.
        identity: Box<CapabilityIdentity>,
    },
    /// Installed descriptor bytes differ from the durable invocation snapshot.
    #[error("installed model provider descriptor differs from durable snapshot")]
    DescriptorMismatch {
        /// Conflicting owner/name/version identity.
        identity: Box<CapabilityIdentity>,
    },
}

struct ToolProviderBinding {
    descriptor: ToolDescriptor,
    provider: Arc<dyn ErasedTool>,
}

/// Startup-only builder for immutable schema-validated tool bindings.
#[derive(Default)]
pub struct ToolProviderRegistryBuilder {
    bindings: BTreeMap<CapabilityIdentity, ToolProviderBinding>,
}

impl ToolProviderRegistryBuilder {
    /// Maximum exact tool revisions installed in one process snapshot.
    pub const MAX_BINDINGS: usize = 16_384;

    /// Creates an empty tool registry builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one already schema-validated erased tool adapter.
    ///
    /// # Errors
    ///
    /// Rejects capacity exhaustion or a repeated owner/name/version identity.
    pub fn register(
        &mut self,
        provider: Arc<dyn ErasedTool>,
    ) -> Result<(), ToolProviderRegistryError> {
        if self.bindings.len() == Self::MAX_BINDINGS {
            return Err(ToolProviderRegistryError::TooManyBindings);
        }
        let descriptor = provider.descriptor().clone();
        let identity = descriptor.metadata().identity().clone();
        if self.bindings.contains_key(&identity) {
            return Err(ToolProviderRegistryError::DuplicateIdentity {
                identity: Box::new(identity),
            });
        }
        self.bindings.insert(
            identity,
            ToolProviderBinding {
                descriptor,
                provider,
            },
        );
        Ok(())
    }

    /// Freezes this startup snapshot. An empty registry is valid for model-only workers.
    #[must_use]
    pub fn build(self) -> ToolProviderRegistry {
        ToolProviderRegistry {
            bindings: Arc::new(self.bindings),
        }
    }
}

impl fmt::Debug for ToolProviderRegistryBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolProviderRegistryBuilder")
            .field("bindings", &self.bindings.len())
            .finish_non_exhaustive()
    }
}

/// Immutable exact-version tool provider registry.
#[derive(Clone)]
pub struct ToolProviderRegistry {
    bindings: Arc<BTreeMap<CapabilityIdentity, ToolProviderBinding>>,
}

impl ToolProviderRegistry {
    /// Resolves and revalidates the exact durable tool descriptor.
    ///
    /// # Errors
    ///
    /// Returns a missing-binding error or fails closed if any descriptor field
    /// differs from the immutable startup snapshot.
    pub fn resolve(
        &self,
        descriptor: &ToolDescriptor,
    ) -> Result<Arc<dyn ErasedTool>, ToolProviderRegistryError> {
        let identity = descriptor.metadata().identity();
        let binding = self.bindings.get(identity).ok_or_else(|| {
            ToolProviderRegistryError::MissingBinding {
                identity: Box::new(identity.clone()),
            }
        })?;
        if &binding.descriptor != descriptor || binding.provider.descriptor() != descriptor {
            return Err(ToolProviderRegistryError::DescriptorMismatch {
                identity: Box::new(identity.clone()),
            });
        }
        Ok(Arc::clone(&binding.provider))
    }

    /// Returns the number of exact tool revisions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.bindings.len()
    }

    /// Returns whether this worker has no tool bindings.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }
}

impl fmt::Debug for ToolProviderRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolProviderRegistry")
            .field("bindings", &self.bindings.len())
            .finish_non_exhaustive()
    }
}

/// Startup or exact-resolution failure for tool adapters.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolProviderRegistryError {
    /// The immutable process binding ceiling was reached.
    #[error("tool provider registry contains too many bindings")]
    TooManyBindings,
    /// One exact owner/name/version identity was registered twice.
    #[error("tool provider identity was registered more than once")]
    DuplicateIdentity {
        /// Repeated exact binding identity.
        identity: Box<CapabilityIdentity>,
    },
    /// No executable adapter owns the durable identity.
    #[error("durable tool descriptor has no installed provider binding")]
    MissingBinding {
        /// Missing exact binding identity.
        identity: Box<CapabilityIdentity>,
    },
    /// Installed descriptor bytes differ from the durable invocation snapshot.
    #[error("installed tool provider descriptor differs from durable snapshot")]
    DescriptorMismatch {
        /// Conflicting owner/name/version identity.
        identity: Box<CapabilityIdentity>,
    },
}

#[cfg(test)]
mod tests {
    use stateknot_core::{
        BoxFuture, BoxStream, ModelContext, ModelError, ModelEvent, ModelRequest, ModelResponse,
        ToolContext, ToolError, ToolInput, ToolResult,
    };

    use super::*;

    struct TestModel {
        descriptor: ModelDescriptor,
    }

    impl Model for TestModel {
        fn descriptor(&self) -> &ModelDescriptor {
            &self.descriptor
        }

        fn invoke(
            &self,
            _: ModelContext,
            _: ModelRequest,
        ) -> BoxFuture<'_, Result<ModelResponse, ModelError>> {
            unimplemented!("provider registry tests never dispatch")
        }

        fn stream(
            &self,
            _: ModelContext,
            _: ModelRequest,
        ) -> BoxStream<'_, Result<ModelEvent, ModelError>> {
            unimplemented!("provider registry tests never dispatch")
        }
    }

    struct TestTool {
        descriptor: ToolDescriptor,
    }

    impl ErasedTool for TestTool {
        fn descriptor(&self) -> &ToolDescriptor {
            &self.descriptor
        }

        fn call(
            &self,
            _: ToolContext,
            _: ToolInput,
        ) -> BoxFuture<'_, Result<ToolResult, ToolError>> {
            unimplemented!("provider registry tests never dispatch")
        }
    }

    fn model_descriptor(name: &str) -> ModelDescriptor {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../stateknot-core/tests/fixtures/core-model-descriptor-v1.json"
        ))
        .unwrap();
        let mut descriptor = fixture["descriptors"]["valid"][0].clone();
        descriptor["metadata"]["identity"]["capability"]["name"] =
            serde_json::Value::String(name.to_owned());
        serde_json::from_value(descriptor).unwrap()
    }

    fn tool_descriptor(name: &str) -> ToolDescriptor {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../stateknot-core/tests/fixtures/core-tool-v1.json"
        ))
        .unwrap();
        let mut descriptor = fixture["descriptors"]["valid"][0].clone();
        descriptor["metadata"]["identity"]["capability"]["name"] =
            serde_json::Value::String(name.to_owned());
        serde_json::from_value(descriptor).unwrap()
    }

    #[test]
    fn model_registry_rejects_duplicates_and_descriptor_substitution() {
        let primary = model_descriptor("models.primary");
        let alternate = model_descriptor("models.alternate");
        let mut builder = ModelProviderRegistryBuilder::new();
        builder
            .register(Arc::new(TestModel {
                descriptor: primary.clone(),
            }))
            .unwrap();
        assert!(matches!(
            builder.register(Arc::new(TestModel {
                descriptor: primary.clone()
            })),
            Err(ModelProviderRegistryError::DuplicateIdentity { .. })
        ));
        let registry = builder.build();
        assert!(registry.resolve(&primary).is_ok());
        assert!(matches!(
            registry.resolve(&alternate),
            Err(ModelProviderRegistryError::MissingBinding { .. })
        ));
    }

    #[test]
    fn tool_registry_rejects_duplicates_and_missing_exact_versions() {
        let primary = tool_descriptor("payments.capture");
        let alternate = tool_descriptor("payments.refund");
        let mut builder = ToolProviderRegistryBuilder::new();
        builder
            .register(Arc::new(TestTool {
                descriptor: primary.clone(),
            }))
            .unwrap();
        assert!(matches!(
            builder.register(Arc::new(TestTool {
                descriptor: primary.clone()
            })),
            Err(ToolProviderRegistryError::DuplicateIdentity { .. })
        ));
        let registry = builder.build();
        assert!(registry.resolve(&primary).is_ok());
        assert!(matches!(
            registry.resolve(&alternate),
            Err(ToolProviderRegistryError::MissingBinding { .. })
        ));
    }
}
