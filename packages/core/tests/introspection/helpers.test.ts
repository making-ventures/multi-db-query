import { describe, expect, it } from 'vitest'
import { resolveApiNameMapper, snakeToCamel } from '../../src/introspection.js'

describe('snakeToCamel', () => {
  it('converts snake_case to camelCase', () => {
    expect(snakeToCamel('user_name')).toBe('userName')
    expect(snakeToCamel('created_at')).toBe('createdAt')
    expect(snakeToCamel('order_id')).toBe('orderId')
  })

  it('handles multiple underscores', () => {
    expect(snakeToCamel('first_last_name')).toBe('firstLastName')
    expect(snakeToCamel('a_b_c_d')).toBe('aBCD')
  })

  it('preserves already camelCase', () => {
    expect(snakeToCamel('userName')).toBe('userName')
    expect(snakeToCamel('id')).toBe('id')
  })

  it('handles leading underscores by capitalizing', () => {
    // Leading _x is treated as _X (same as mid-word)
    expect(snakeToCamel('_private')).toBe('Private')
  })

  it('handles numeric suffixes', () => {
    expect(snakeToCamel('col_1')).toBe('col1')
    expect(snakeToCamel('item_2_name')).toBe('item2Name')
  })
})

describe('resolveApiNameMapper', () => {
  it('defaults to snakeToCamel for undefined', () => {
    const mapper = resolveApiNameMapper(undefined)
    expect(mapper('user_name')).toBe('userName')
  })

  it('returns snakeToCamel for "camelCase"', () => {
    const mapper = resolveApiNameMapper('camelCase')
    expect(mapper('user_name')).toBe('userName')
  })

  it('returns identity for "preserve"', () => {
    const mapper = resolveApiNameMapper('preserve')
    expect(mapper('user_name')).toBe('user_name')
  })

  it('returns custom function as-is', () => {
    const custom = (s: string) => s.toUpperCase()
    const mapper = resolveApiNameMapper(custom)
    expect(mapper('user_name')).toBe('USER_NAME')
  })
})
