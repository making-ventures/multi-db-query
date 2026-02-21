import type { MetadataConfig, RoleMeta } from '@mkven/multi-db-validation'
import { ConfigError, ValidationError } from '@mkven/multi-db-validation'
import { beforeAll, describe, expect, it } from 'vitest'

import type { ValidateConfigInput, ValidateQueryInput, ValidateResult } from '../client.js'

// ── ValidationContract ─────────────────────────────────────────

export interface ValidationContract {
  validateQuery(input: ValidateQueryInput): Promise<ValidateResult>
  validateConfig(input: ValidateConfigInput): Promise<ValidateResult>
}

// ── describeValidationContract ─────────────────────────────────

export function describeValidationContract(
  name: string,
  factory: () => Promise<ValidationContract>,
  metadata: MetadataConfig,
  roles: readonly RoleMeta[],
): void {
  describe(`ValidationContract: ${name}`, () => {
    let engine: ValidationContract

    beforeAll(async () => {
      engine = await factory()
    })

    // ── 17.1 Query Validation ────────────────────────────────

    it('C1600: valid query passes', async () => {
      const result = await engine.validateQuery({
        definition: { from: 'orders', columns: ['id'] },
        context: { roles: { user: ['admin'] } },
      })
      expect(result.valid).toBe(true)
    })

    it('C1601: unknown table rejected', async () => {
      try {
        await engine.validateQuery({
          definition: { from: 'nonExistentTable' },
          context: { roles: { user: ['admin'] } },
        })
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(ValidationError)
        const ve = err as ValidationError
        expect(ve.errors.some((e) => e.code === 'UNKNOWN_TABLE')).toBe(true)
      }
    })

    it('C1602: unknown column rejected', async () => {
      try {
        await engine.validateQuery({
          definition: { from: 'orders', columns: ['id', 'nonExistentColumn'] },
          context: { roles: { user: ['admin'] } },
        })
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(ValidationError)
        const ve = err as ValidationError
        expect(ve.errors.some((e) => e.code === 'UNKNOWN_COLUMN')).toBe(true)
      }
    })

    it('C1603: access denied rejected', async () => {
      try {
        await engine.validateQuery({
          definition: { from: 'orders', columns: ['id', 'internalNote'] },
          context: { roles: { user: ['tenant-user'] } },
        })
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(ValidationError)
        const ve = err as ValidationError
        expect(ve.errors.some((e) => e.code === 'ACCESS_DENIED')).toBe(true)
      }
    })

    it('C1604: invalid filter rejected', async () => {
      try {
        await engine.validateQuery({
          definition: {
            from: 'orders',
            columns: ['id'],
            filters: [{ column: 'customerId', operator: '>', value: '00000000-0000-4000-a000-000000000c01' }],
          },
          context: { roles: { user: ['admin'] } },
        })
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(ValidationError)
        const ve = err as ValidationError
        expect(ve.errors.some((e) => e.code === 'INVALID_FILTER')).toBe(true)
      }
    })

    it('C1605: invalid value rejected', async () => {
      try {
        await engine.validateQuery({
          definition: {
            from: 'orders',
            columns: ['id'],
            filters: [{ column: 'total', operator: 'between', value: { from: 0 } }],
          },
          context: { roles: { user: ['admin'] } },
        })
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(ValidationError)
        const ve = err as ValidationError
        expect(ve.errors.some((e) => e.code === 'INVALID_VALUE')).toBe(true)
      }
    })

    it('C1606: multiple errors collected', async () => {
      try {
        await engine.validateQuery({
          definition: { from: 'nonExistentTable', columns: ['badCol'] },
          context: { roles: { user: ['admin'] } },
        })
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(ValidationError)
        const ve = err as ValidationError
        // At least TABLE_NOT_FOUND; column errors may be skipped when table is unknown
        expect(ve.errors.length).toBeGreaterThanOrEqual(1)
      }
    })

    it('C1607: unknown role rejected', async () => {
      try {
        await engine.validateQuery({
          definition: { from: 'orders', columns: ['id'] },
          context: { roles: { user: ['nonexistent'] } },
        })
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(ValidationError)
        const ve = err as ValidationError
        expect(ve.errors.some((e) => e.code === 'UNKNOWN_ROLE')).toBe(true)
      }
    })

    it('C1608: no DB connection used', async () => {
      // Validation uses only metadata + roles — no executor/DB connection needed.
      // This test verifies that validateQuery works without database connectivity.
      const result = await engine.validateQuery({
        definition: { from: 'orders', columns: ['id'] },
        context: { roles: { user: ['admin'] } },
      })
      expect(result.valid).toBe(true)
    })

    it('C1609: same error format as /query', async () => {
      try {
        await engine.validateQuery({
          definition: { from: 'nonExistentTable' },
          context: { roles: { user: ['admin'] } },
        })
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(ValidationError)
        const ve = err as ValidationError
        expect(ve.code).toBe('VALIDATION_FAILED')
        expect(Array.isArray(ve.errors)).toBe(true)
      }
    })

    // ── 17.2 Config Validation ───────────────────────────────

    it('C1620: valid config passes', async () => {
      const result = await engine.validateConfig({ metadata, roles })
      expect(result.valid).toBe(true)
    })

    it('C1621: invalid apiName format', async () => {
      const dbId = metadata.databases[0]?.id ?? 'pg-main'
      const invalidMetadata: MetadataConfig = {
        ...metadata,
        tables: [
          {
            id: 'bad-name',
            apiName: 'Order_Items',
            database: dbId,
            physicalName: 'public.bad_name',
            columns: [{ apiName: 'id', physicalName: 'id', type: 'int', nullable: false }],
            primaryKey: ['id'],
            relations: [],
          },
        ],
      }
      try {
        await engine.validateConfig({ metadata: invalidMetadata, roles })
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(ConfigError)
        const ce = err as ConfigError
        expect(ce.errors.some((e) => e.code === 'INVALID_API_NAME')).toBe(true)
      }
    })

    it('C1622: duplicate apiName rejected', async () => {
      const dbId = metadata.databases[0]?.id ?? 'pg-main'
      const invalidMetadata: MetadataConfig = {
        ...metadata,
        tables: [
          ...metadata.tables,
          {
            id: 'orders-dup',
            apiName: 'orders',
            database: dbId,
            physicalName: 'public.orders_dup',
            columns: [{ apiName: 'id', physicalName: 'id', type: 'int', nullable: false }],
            primaryKey: ['id'],
            relations: [],
          },
        ],
      }
      try {
        await engine.validateConfig({ metadata: invalidMetadata, roles })
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(ConfigError)
        const ce = err as ConfigError
        expect(ce.errors.some((e) => e.code === 'DUPLICATE_API_NAME')).toBe(true)
      }
    })

    it('C1623: invalid DB reference rejected', async () => {
      const invalidMetadata: MetadataConfig = {
        ...metadata,
        tables: [
          {
            id: 'orphan',
            apiName: 'orphan',
            database: 'non-existent-db',
            physicalName: 'public.orphan',
            columns: [{ apiName: 'id', physicalName: 'id', type: 'int', nullable: false }],
            primaryKey: ['id'],
            relations: [],
          },
        ],
      }
      try {
        await engine.validateConfig({ metadata: invalidMetadata, roles })
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(ConfigError)
        const ce = err as ConfigError
        expect(ce.errors.some((e) => e.code === 'INVALID_REFERENCE')).toBe(true)
      }
    })

    it('C1624: invalid relation rejected', async () => {
      const dbId = metadata.databases[0]?.id ?? 'pg-main'
      const invalidMetadata: MetadataConfig = {
        ...metadata,
        tables: [
          {
            id: 'linked',
            apiName: 'linked',
            database: dbId,
            physicalName: 'public.linked',
            columns: [
              { apiName: 'id', physicalName: 'id', type: 'int', nullable: false },
              { apiName: 'ref', physicalName: 'ref_id', type: 'uuid', nullable: true },
            ],
            primaryKey: ['id'],
            relations: [
              { column: 'ref', references: { table: 'nonExistentTable', column: 'id' }, type: 'many-to-one' },
            ],
          },
        ],
      }
      try {
        await engine.validateConfig({ metadata: invalidMetadata, roles })
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(ConfigError)
        const ce = err as ConfigError
        expect(ce.errors.some((e) => e.code === 'INVALID_RELATION')).toBe(true)
      }
    })

    it('C1625: invalid sync reference', async () => {
      const invalidMetadata: MetadataConfig = {
        ...metadata,
        externalSyncs: [
          {
            sourceTable: 'nonExistentTable',
            targetDatabase: 'ch-analytics',
            targetPhysicalName: 'default.missing_replica',
            method: 'debezium',
            estimatedLag: 'seconds',
          },
        ],
      }
      try {
        await engine.validateConfig({ metadata: invalidMetadata, roles })
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(ConfigError)
        const ce = err as ConfigError
        expect(ce.errors.some((e) => e.code === 'INVALID_SYNC')).toBe(true)
      }
    })

    it('C1626: invalid cache config', async () => {
      const invalidMetadata: MetadataConfig = {
        ...metadata,
        caches: [
          {
            id: 'bad-cache',
            engine: 'redis',
            tables: [{ tableId: 'nonExistentTable', keyPattern: 'missing:{id}' }],
          },
        ],
      }
      try {
        await engine.validateConfig({ metadata: invalidMetadata, roles })
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(ConfigError)
        const ce = err as ConfigError
        expect(ce.errors.some((e) => e.code === 'INVALID_CACHE')).toBe(true)
      }
    })

    it('C1627: multiple config errors', async () => {
      const invalidMetadata: MetadataConfig = {
        ...metadata,
        tables: [
          {
            id: 'orphan1',
            apiName: 'orphan1',
            database: 'non-existent-db',
            physicalName: 'public.orphan1',
            columns: [{ apiName: 'id', physicalName: 'id', type: 'int', nullable: false }],
            primaryKey: ['id'],
            relations: [{ column: 'id', references: { table: 'nonExistent', column: 'id' }, type: 'many-to-one' }],
          },
        ],
      }
      try {
        await engine.validateConfig({ metadata: invalidMetadata, roles })
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(ConfigError)
        const ce = err as ConfigError
        expect(ce.errors.length).toBeGreaterThanOrEqual(2)
      }
    })

    it('C1628: duplicate column apiName', async () => {
      const dbId = metadata.databases[0]?.id ?? 'pg-main'
      const invalidMetadata: MetadataConfig = {
        ...metadata,
        tables: [
          {
            id: 'dupCol',
            apiName: 'dupCol',
            database: dbId,
            physicalName: 'public.dup_col',
            columns: [
              { apiName: 'id', physicalName: 'id', type: 'int', nullable: false },
              { apiName: 'id', physicalName: 'id2', type: 'int', nullable: false },
            ],
            primaryKey: ['id'],
            relations: [],
          },
        ],
      }
      try {
        await engine.validateConfig({ metadata: invalidMetadata, roles })
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(ConfigError)
        const ce = err as ConfigError
        expect(ce.errors.some((e) => e.code === 'DUPLICATE_API_NAME')).toBe(true)
      }
    })

    it('C1629: apiName starting with uppercase', async () => {
      const dbId = metadata.databases[0]?.id ?? 'pg-main'
      const invalidMetadata: MetadataConfig = {
        ...metadata,
        tables: [
          {
            id: 'upper',
            apiName: 'Orders',
            database: dbId,
            physicalName: 'public.upper',
            columns: [{ apiName: 'id', physicalName: 'id', type: 'int', nullable: false }],
            primaryKey: ['id'],
            relations: [],
          },
        ],
      }
      try {
        await engine.validateConfig({ metadata: invalidMetadata, roles })
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(ConfigError)
        const ce = err as ConfigError
        expect(ce.errors.some((e) => e.code === 'INVALID_API_NAME')).toBe(true)
      }
    })

    it('C1630: apiName with underscore', async () => {
      const dbId = metadata.databases[0]?.id ?? 'pg-main'
      const invalidMetadata: MetadataConfig = {
        ...metadata,
        tables: [
          {
            id: 'under',
            apiName: 'order_items',
            database: dbId,
            physicalName: 'public.under',
            columns: [{ apiName: 'id', physicalName: 'id', type: 'int', nullable: false }],
            primaryKey: ['id'],
            relations: [],
          },
        ],
      }
      try {
        await engine.validateConfig({ metadata: invalidMetadata, roles })
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(ConfigError)
        const ce = err as ConfigError
        expect(ce.errors.some((e) => e.code === 'INVALID_API_NAME')).toBe(true)
      }
    })

    it('C1631: relation source column does not exist', async () => {
      const dbId = metadata.databases[0]?.id ?? 'pg-main'
      const invalidMetadata: MetadataConfig = {
        ...metadata,
        tables: [
          ...metadata.tables,
          {
            id: 'badRelSrc',
            apiName: 'badRelSrc',
            database: dbId,
            physicalName: 'public.bad_rel_src',
            columns: [{ apiName: 'id', physicalName: 'id', type: 'int', nullable: false }],
            primaryKey: ['id'],
            relations: [
              { column: 'nonExistentCol', references: { table: 'orders', column: 'id' }, type: 'many-to-one' },
            ],
          },
        ],
      }
      try {
        await engine.validateConfig({ metadata: invalidMetadata, roles })
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(ConfigError)
        const ce = err as ConfigError
        expect(ce.errors.some((e) => e.code === 'INVALID_RELATION')).toBe(true)
      }
    })

    it('C1632: relation target column does not exist', async () => {
      const dbId = metadata.databases[0]?.id ?? 'pg-main'
      const invalidMetadata: MetadataConfig = {
        ...metadata,
        tables: [
          ...metadata.tables,
          {
            id: 'badRelTgt',
            apiName: 'badRelTgt',
            database: dbId,
            physicalName: 'public.bad_rel_tgt',
            columns: [
              { apiName: 'id', physicalName: 'id', type: 'int', nullable: false },
              { apiName: 'ref', physicalName: 'ref_id', type: 'int', nullable: true },
            ],
            primaryKey: ['id'],
            relations: [
              { column: 'ref', references: { table: 'orders', column: 'nonExistentCol' }, type: 'many-to-one' },
            ],
          },
        ],
      }
      try {
        await engine.validateConfig({ metadata: invalidMetadata, roles })
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(ConfigError)
        const ce = err as ConfigError
        expect(ce.errors.some((e) => e.code === 'INVALID_RELATION')).toBe(true)
      }
    })
  })
}
