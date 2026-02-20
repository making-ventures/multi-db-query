import type { MetadataConfig, RoleMeta } from '@mkven/multi-db-validation'
import type { MetadataProvider, RoleProvider } from '../types/providers.js'

/**
 * Creates a MetadataProvider that always returns the same config.
 */
export function staticMetadata(config: MetadataConfig): MetadataProvider {
  return {
    load: () => Promise.resolve(config),
  }
}

/**
 * Creates a RoleProvider that always returns the same roles.
 */
export function staticRoles(roles: RoleMeta[]): RoleProvider {
  return {
    load: () => Promise.resolve(roles),
  }
}
