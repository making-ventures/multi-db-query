/**
 * Shared dialect test runner — tests the same IR inputs against all 3 dialects
 * with per-dialect expected SQL/param assertions.
 *
 * Each DialectTestConfig provides expected output strings for a fixed set of test cases.
 * Tests that are truly unique to one dialect (e.g. Trino catalog, CH named-param typing)
 * stay in their own files.
 */
import { describe, expect, it } from 'vitest'
import type { SqlDialect } from '../../src/dialects/dialect.js'
import type {
  AggregationClause,
  HavingGroup,
  SqlParts,
  WhereArrayCondition,
  WhereBetween,
  WhereColumnCondition,
  WhereCondition,
  WhereCountedSubquery,
  WhereExists,
  WhereFunction,
  WhereGroup,
} from '../../src/types/ir.js'
import { col, makeBase, tbl } from './helpers.js'

// ── Config types ───────────────────────────────────────────────

export interface Expect {
  /** Strings the SQL must contain */
  sql: string[]
  /** Strings the SQL must NOT contain (optional) */
  notSql?: string[]
  /** Expected params array (optional) */
  params?: unknown[]
}

export interface DialectTestConfig extends Record<string, Expect | string> {
  /** Display name, e.g. 'PostgresDialect' */
  name: string
  /** Default schema for base(), e.g. 'public' or 'default' */
  schema: string
  /** Child-table schema for subquery tests, e.g. 'public' or 'default' */
  subSchema: string

  // ── SELECT ─────────────────────────────────────────────
  simpleSelect: Expect
  distinct: Expect
  countMode: Expect
  emptySelect: Expect

  // ── JOIN ───────────────────────────────────────────────
  innerJoin: Expect
  leftJoin: Expect

  // ── WHERE standard ─────────────────────────────────────
  equals: Expect
  notEquals: Expect
  lessThan: Expect
  greaterThanOrEqual: Expect

  // ── NULL ───────────────────────────────────────────────
  isNull: Expect
  isNotNull: Expect

  // ── LIKE ───────────────────────────────────────────────
  like: Expect
  notLike: Expect

  // ── Pattern wrapping ───────────────────────────────────
  startsWith: Expect
  endsWith: Expect
  contains: Expect
  notContains: Expect

  // ── Case-insensitive ───────────────────────────────────
  ilike: Expect
  notIlike: Expect
  istartsWith: Expect
  iendsWith: Expect
  icontains: Expect
  notIcontains: Expect

  // ── Wildcard escaping ──────────────────────────────────
  escapesPercent: Expect
  escapesUnderscore: Expect
  escapesBackslash: Expect

  // ── BETWEEN ────────────────────────────────────────────
  between: Expect
  notBetween: Expect

  // ── Levenshtein ────────────────────────────────────────
  levenshtein: Expect

  // ── Array operators ────────────────────────────────────
  arrayContains: Expect
  arrayContainsAll: Expect
  arrayContainsAny: Expect
  arrayIsEmpty: Expect
  arrayIsNotEmpty: Expect

  // ── Column comparison ──────────────────────────────────
  columnComparison: Expect

  // ── Groups ─────────────────────────────────────────────
  orGroup: Expect
  andGroup: Expect
  notGroup: Expect
  singleElementGroup: Expect

  // ── EXISTS ─────────────────────────────────────────────
  exists: Expect
  notExists: Expect
  existsWithSubFilters: Expect

  // ── Counted subquery ───────────────────────────────────
  countedGte: Expect
  countedGt: Expect
  countedLt: Expect
  countedLte: Expect

  // ── GROUP BY + aggregations ────────────────────────────
  groupByCount: Expect
  sumAgg: Expect
  avgMinMax: Expect

  // ── HAVING ─────────────────────────────────────────────
  having: Expect
  havingBetween: Expect
  havingNotBetween: Expect

  // ── ORDER BY ───────────────────────────────────────────
  orderAsc: Expect
  orderDesc: Expect
  orderAggAlias: Expect
  multipleOrder: Expect

  // ── LIMIT / OFFSET ─────────────────────────────────────
  limit: Expect
  offset: Expect
  limitOffset: Expect

  // ── IN / NOT IN ────────────────────────────────────────
  inUuid: Expect
  notInString: Expect
  inInt: Expect
  inDefaultType: Expect
  inSingleElement: Expect

  // ── Type casts (via IN) ────────────────────────────────
  typeCastDecimal: Expect

  // ── Float param ────────────────────────────────────────
  floatParam: Expect

  // ── Catalog-qualified ──────────────────────────────────
  catalogTable: Expect
  catalogJoin: Expect

  // ── HAVING (extended) ──────────────────────────────────
  havingAndGroup: Expect
  havingOrGroup: Expect
  havingNotGroup: Expect
  havingIsNull: Expect

  // ── Complex WHERE ──────────────────────────────────────
  existsInsideOrGroup: Expect
  deeplyNestedWhere: Expect
  mixedFilterGroupExists: Expect
  countedWithSubFilters: Expect

  // ── Nested EXISTS ──────────────────────────────────────
  nestedExists: Expect

  // ── Join-related ───────────────────────────────────────
  filterOnJoinedColumn: Expect
  threeTableJoin: Expect
  multiJoinPerTableFilters: Expect
  aggOnJoinedColumn: Expect

  // ── Cross-table ORDER BY ───────────────────────────────
  crossTableOrderBy: Expect

  // ── Array ops on int[] ─────────────────────────────────
  arrayContainsInt: Expect
  arrayContainsAllInt: Expect
  arrayInGroup: Expect
  arrayOnJoinedTable: Expect
  arrayContainsAllSingleElement: Expect

  // ── distinct + groupBy ─────────────────────────────────
  distinctGroupBy: Expect

  // ── Full query ─────────────────────────────────────────
  fullQuery: Expect
  paramOrder: Expect

  // ── Injection defense-in-depth ─────────────────────────
  injectionAggAlias: Expect
  injectionOrderByAlias: Expect
  injectionHavingAlias: Expect
  injectionSafeAggFn: Expect
  injectionWhereStringColumn: Expect
}

// ── Runner ─────────────────────────────────────────────────────

export function describeSharedDialectTests(dialect: SqlDialect, cfg: DialectTestConfig): void {
  const base = makeBase(cfg.schema)
  const sub = cfg.subSchema

  function check(parts: SqlParts, params: unknown[], exp: Expect): void {
    const result = dialect.generate(parts, params)
    for (const s of exp.sql) expect(result.sql).toContain(s)
    for (const s of exp.notSql ?? []) expect(result.sql).not.toContain(s)
    if (exp.params !== undefined) expect(result.params).toEqual(exp.params)
  }

  describe(`${cfg.name} — shared dialect tests`, () => {
    // ── SELECT ────────────────────────────────────────────

    describe('SELECT', () => {
      it('simple select', () => check(base(), [], cfg.simpleSelect))
      it('distinct', () => check(base({ distinct: true }), [], cfg.distinct))
      it('count mode', () => check(base({ select: [], countMode: true }), [], cfg.countMode))
      it('empty select → *', () => check(base({ select: [] }), [], cfg.emptySelect))
    })

    // ── JOIN ──────────────────────────────────────────────

    describe('JOIN', () => {
      it('inner join', () => {
        const parts = base({
          joins: [
            {
              type: 'inner',
              table: tbl(`${sub}.orders`, 't1'),
              leftColumn: col('t0', 'id'),
              rightColumn: col('t1', 'user_id'),
            },
          ],
        })
        check(parts, [], cfg.innerJoin)
      })

      it('left join', () => {
        const parts = base({
          joins: [
            {
              type: 'left',
              table: tbl(`${sub}.orders`, 't1'),
              leftColumn: col('t0', 'id'),
              rightColumn: col('t1', 'user_id'),
            },
          ],
        })
        check(parts, [], cfg.leftJoin)
      })
    })

    // ── WHERE standard ────────────────────────────────────

    describe('WHERE standard', () => {
      it('equals', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: '=', paramIndex: 0 }
        check(base({ where: cond }), ['Alice'], cfg.equals)
      })

      it('not equals', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: '!=', paramIndex: 0 }
        check(base({ where: cond }), ['Alice'], cfg.notEquals)
      })

      it('less than', () => {
        const cond: WhereCondition = { column: col('t0', 'age'), operator: '<', paramIndex: 0 }
        check(base({ where: cond }), [30], cfg.lessThan)
      })

      it('greater than or equal', () => {
        const cond: WhereCondition = { column: col('t0', 'age'), operator: '>=', paramIndex: 0 }
        check(base({ where: cond }), [18], cfg.greaterThanOrEqual)
      })
    })

    // ── NULL ──────────────────────────────────────────────

    describe('NULL checks', () => {
      it('IS NULL', () => {
        const cond: WhereCondition = { column: col('t0', 'age'), operator: 'isNull' }
        check(base({ where: cond }), [], cfg.isNull)
      })

      it('IS NOT NULL', () => {
        const cond: WhereCondition = { column: col('t0', 'age'), operator: 'isNotNull' }
        check(base({ where: cond }), [], cfg.isNotNull)
      })
    })

    // ── LIKE ──────────────────────────────────────────────

    describe('LIKE', () => {
      it('like raw pattern', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'like', paramIndex: 0 }
        check(base({ where: cond }), ['%foo%'], cfg.like)
      })

      it('notLike', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'notLike', paramIndex: 0 }
        check(base({ where: cond }), ['%x%'], cfg.notLike)
      })
    })

    // ── Pattern wrapping ──────────────────────────────────

    describe('pattern wrapping', () => {
      it('startsWith', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'startsWith', paramIndex: 0 }
        check(base({ where: cond }), ['Ali'], cfg.startsWith)
      })

      it('endsWith', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'endsWith', paramIndex: 0 }
        check(base({ where: cond }), ['ice'], cfg.endsWith)
      })

      it('contains', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'contains', paramIndex: 0 }
        check(base({ where: cond }), ['lic'], cfg.contains)
      })

      it('notContains', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'notContains', paramIndex: 0 }
        check(base({ where: cond }), ['bad'], cfg.notContains)
      })
    })

    // ── Case-insensitive ──────────────────────────────────

    describe('case-insensitive', () => {
      it('ilike', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'ilike', paramIndex: 0 }
        check(base({ where: cond }), ['%foo%'], cfg.ilike)
      })

      it('notIlike', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'notIlike', paramIndex: 0 }
        check(base({ where: cond }), ['%x%'], cfg.notIlike)
      })

      it('istartsWith', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'istartsWith', paramIndex: 0 }
        check(base({ where: cond }), ['ali'], cfg.istartsWith)
      })

      it('iendsWith', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'iendsWith', paramIndex: 0 }
        check(base({ where: cond }), ['ICE'], cfg.iendsWith)
      })

      it('icontains', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'icontains', paramIndex: 0 }
        check(base({ where: cond }), ['LIC'], cfg.icontains)
      })

      it('notIcontains', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'notIcontains', paramIndex: 0 }
        check(base({ where: cond }), ['BAD'], cfg.notIcontains)
      })
    })

    // ── Wildcard escaping ─────────────────────────────────

    describe('wildcard escaping', () => {
      it('escapes %', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'contains', paramIndex: 0 }
        check(base({ where: cond }), ['100%'], cfg.escapesPercent)
      })

      it('escapes _', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'contains', paramIndex: 0 }
        check(base({ where: cond }), ['a_b'], cfg.escapesUnderscore)
      })

      it('escapes backslash', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'contains', paramIndex: 0 }
        check(base({ where: cond }), ['a\\b'], cfg.escapesBackslash)
      })
    })

    // ── BETWEEN ───────────────────────────────────────────

    describe('BETWEEN', () => {
      it('between', () => {
        const cond: WhereBetween = { column: col('t0', 'age'), fromParamIndex: 0, toParamIndex: 1 }
        check(base({ where: cond }), [18, 65], cfg.between)
      })

      it('not between', () => {
        const cond: WhereBetween = { column: col('t0', 'age'), not: true, fromParamIndex: 0, toParamIndex: 1 }
        check(base({ where: cond }), [0, 17], cfg.notBetween)
      })
    })

    // ── Levenshtein ───────────────────────────────────────

    describe('levenshtein', () => {
      it('levenshtein distance', () => {
        const cond: WhereFunction = {
          fn: 'levenshtein',
          column: col('t0', 'name'),
          fnParamIndex: 0,
          operator: '<=',
          compareParamIndex: 1,
        }
        check(base({ where: cond }), ['test', 2], cfg.levenshtein)
      })
    })

    // ── Array operators ───────────────────────────────────

    describe('array operators', () => {
      it('arrayContains', () => {
        const cond: WhereArrayCondition = {
          column: col('t0', 'tags'),
          operator: 'contains',
          paramIndexes: [0],
          elementType: 'string',
        }
        check(base({ where: cond }), ['urgent'], cfg.arrayContains)
      })

      it('arrayContainsAll', () => {
        const cond: WhereArrayCondition = {
          column: col('t0', 'tags'),
          operator: 'containsAll',
          paramIndexes: [0],
          elementType: 'string',
        }
        check(base({ where: cond }), [['a', 'b']], cfg.arrayContainsAll)
      })

      it('arrayContainsAny', () => {
        const cond: WhereArrayCondition = {
          column: col('t0', 'tags'),
          operator: 'containsAny',
          paramIndexes: [0],
          elementType: 'string',
        }
        check(base({ where: cond }), [['x', 'y']], cfg.arrayContainsAny)
      })

      it('arrayIsEmpty', () => {
        const cond: WhereArrayCondition = { column: col('t0', 'tags'), operator: 'isEmpty', elementType: 'string' }
        check(base({ where: cond }), [], cfg.arrayIsEmpty)
      })

      it('arrayIsNotEmpty', () => {
        const cond: WhereArrayCondition = { column: col('t0', 'tags'), operator: 'isNotEmpty', elementType: 'string' }
        check(base({ where: cond }), [], cfg.arrayIsNotEmpty)
      })
    })

    // ── Column comparison ─────────────────────────────────

    describe('column comparison', () => {
      it('column-to-column', () => {
        const cond: WhereColumnCondition = { leftColumn: col('t0', 'a'), operator: '>', rightColumn: col('t1', 'b') }
        check(base({ where: cond }), [], cfg.columnComparison)
      })
    })

    // ── Groups ────────────────────────────────────────────

    describe('groups', () => {
      it('OR group', () => {
        const cond: WhereGroup = {
          logic: 'or',
          conditions: [
            { column: col('t0', 'name'), operator: '=', paramIndex: 0 } as WhereCondition,
            { column: col('t0', 'name'), operator: '=', paramIndex: 1 } as WhereCondition,
          ],
        }
        check(base({ where: cond }), ['Alice', 'Bob'], cfg.orGroup)
      })

      it('AND group', () => {
        const cond: WhereGroup = {
          logic: 'and',
          conditions: [
            { column: col('t0', 'age'), operator: '>=', paramIndex: 0 } as WhereCondition,
            { column: col('t0', 'age'), operator: '<=', paramIndex: 1 } as WhereCondition,
          ],
        }
        check(base({ where: cond }), [18, 65], cfg.andGroup)
      })

      it('NOT group', () => {
        const cond: WhereGroup = {
          logic: 'or',
          not: true,
          conditions: [
            { column: col('t0', 'name'), operator: '=', paramIndex: 0 } as WhereCondition,
            { column: col('t0', 'name'), operator: '=', paramIndex: 1 } as WhereCondition,
          ],
        }
        check(base({ where: cond }), ['x', 'y'], cfg.notGroup)
      })

      it('single-element group — no parens', () => {
        const cond: WhereGroup = {
          logic: 'and',
          conditions: [{ column: col('t0', 'age'), operator: '>', paramIndex: 0 } as WhereCondition],
        }
        check(base({ where: cond }), [10], cfg.singleElementGroup)
      })
    })

    // ── EXISTS ────────────────────────────────────────────

    describe('EXISTS', () => {
      it('EXISTS', () => {
        const cond: WhereExists = {
          exists: true,
          subquery: {
            from: tbl(`${sub}.orders`, 's0'),
            join: { leftColumn: col('t0', 'id'), rightColumn: col('s0', 'user_id') },
          },
        }
        check(base({ where: cond }), [], cfg.exists)
      })

      it('NOT EXISTS', () => {
        const cond: WhereExists = {
          exists: false,
          subquery: {
            from: tbl(`${sub}.orders`, 's0'),
            join: { leftColumn: col('t0', 'id'), rightColumn: col('s0', 'user_id') },
          },
        }
        check(base({ where: cond }), [], cfg.notExists)
      })

      it('EXISTS with sub-filters', () => {
        const subWhere: WhereCondition = { column: col('s0', 'status'), operator: '=', paramIndex: 0 }
        const cond: WhereExists = {
          exists: true,
          subquery: {
            from: tbl(`${sub}.orders`, 's0'),
            join: { leftColumn: col('t0', 'id'), rightColumn: col('s0', 'user_id') },
            where: subWhere,
          },
        }
        check(base({ where: cond }), ['active'], cfg.existsWithSubFilters)
      })
    })

    // ── Counted subquery ──────────────────────────────────

    describe('counted subquery', () => {
      function countedCond(op: string, val: number): { parts: SqlParts; params: unknown[] } {
        const cond: WhereCountedSubquery = {
          subquery: {
            from: tbl(`${sub}.orders`, 's0'),
            join: { leftColumn: col('t0', 'id'), rightColumn: col('s0', 'user_id') },
          },
          operator: op,
          countParamIndex: 0,
        }
        return { parts: base({ where: cond }), params: [val] }
      }

      it('counted >=', () => {
        const { parts, params } = countedCond('>=', 5)
        check(parts, params, cfg.countedGte)
      })

      it('counted >', () => {
        const { parts, params } = countedCond('>', 1)
        check(parts, params, cfg.countedGt)
      })

      it('counted <', () => {
        const { parts, params } = countedCond('<', 2)
        check(parts, params, cfg.countedLt)
      })

      it('counted <=', () => {
        const { parts, params } = countedCond('<=', 1)
        check(parts, params, cfg.countedLte)
      })
    })

    // ── GROUP BY + aggregations ───────────────────────────

    describe('GROUP BY + aggregations', () => {
      it('group by with count', () => {
        const parts = base({
          select: [col('t0', 'status')],
          from: tbl(`${sub}.orders`, 't0'),
          groupBy: [col('t0', 'status')],
          aggregations: [{ fn: 'count', column: '*', alias: 'cnt' }],
        })
        check(parts, [], cfg.groupByCount)
      })

      it('sum', () => {
        const parts = base({
          select: [col('t0', 'status')],
          from: tbl(`${sub}.orders`, 't0'),
          groupBy: [col('t0', 'status')],
          aggregations: [{ fn: 'sum', column: col('t0', 'total'), alias: 'total' }],
        })
        check(parts, [], cfg.sumAgg)
      })

      it('avg, min, max', () => {
        const parts = base({
          select: [],
          from: tbl(`${sub}.orders`, 't0'),
          aggregations: [
            { fn: 'avg', column: col('t0', 'total'), alias: 'avg_amount' },
            { fn: 'min', column: col('t0', 'total'), alias: 'min_amount' },
            { fn: 'max', column: col('t0', 'total'), alias: 'max_amount' },
          ],
        })
        check(parts, [], cfg.avgMinMax)
      })
    })

    // ── HAVING ────────────────────────────────────────────

    describe('HAVING', () => {
      it('simple having', () => {
        const parts = base({
          select: [col('t0', 'status')],
          from: tbl(`${sub}.orders`, 't0'),
          groupBy: [col('t0', 'status')],
          aggregations: [{ fn: 'count', column: '*', alias: 'cnt' }],
          having: { column: 'cnt', operator: '>', paramIndex: 0 },
        })
        check(parts, [5], cfg.having)
      })

      it('having between', () => {
        const parts = base({
          select: [col('t0', 'status')],
          from: tbl(`${sub}.orders`, 't0'),
          groupBy: [col('t0', 'status')],
          aggregations: [{ fn: 'sum', column: col('t0', 'total'), alias: 'total' }],
          having: { alias: 'total', fromParamIndex: 0, toParamIndex: 1 },
        })
        check(parts, [100, 1000], cfg.havingBetween)
      })

      it('having not between', () => {
        const parts = base({
          select: [col('t0', 'status')],
          from: tbl(`${sub}.orders`, 't0'),
          groupBy: [col('t0', 'status')],
          aggregations: [{ fn: 'sum', column: col('t0', 'total'), alias: 'total' }],
          having: { alias: 'total', not: true, fromParamIndex: 0, toParamIndex: 1 },
        })
        check(parts, [100, 1000], cfg.havingNotBetween)
      })
    })

    // ── ORDER BY ──────────────────────────────────────────

    describe('ORDER BY', () => {
      it('asc', () => check(base({ orderBy: [{ column: col('t0', 'name'), direction: 'asc' }] }), [], cfg.orderAsc))
      it('desc', () => check(base({ orderBy: [{ column: col('t0', 'age'), direction: 'desc' }] }), [], cfg.orderDesc))

      it('aggregation alias', () => {
        const parts = base({
          select: [col('t0', 'status')],
          from: tbl(`${sub}.orders`, 't0'),
          groupBy: [col('t0', 'status')],
          aggregations: [{ fn: 'count', column: '*', alias: 'cnt' }],
          orderBy: [{ column: 'cnt', direction: 'desc' }],
        })
        check(parts, [], cfg.orderAggAlias)
      })

      it('multiple order by', () => {
        const parts = base({
          orderBy: [
            { column: col('t0', 'name'), direction: 'asc' },
            { column: col('t0', 'age'), direction: 'desc' },
          ],
        })
        check(parts, [], cfg.multipleOrder)
      })
    })

    // ── LIMIT / OFFSET ────────────────────────────────────

    describe('LIMIT / OFFSET', () => {
      it('limit', () => check(base({ limit: 10 }), [], cfg.limit))
      it('offset', () => check(base({ offset: 20 }), [], cfg.offset))
      it('limit + offset', () => check(base({ limit: 10, offset: 20 }), [], cfg.limitOffset))
    })

    // ── IN / NOT IN ───────────────────────────────────────

    describe('IN / NOT IN', () => {
      it('in with uuid type', () => {
        const cond: WhereCondition = { column: col('t0', 'id'), operator: 'in', paramIndex: 0, columnType: 'uuid' }
        check(base({ where: cond }), [['id1', 'id2']], cfg.inUuid)
      })

      it('notIn with string type', () => {
        const cond: WhereCondition = {
          column: col('t0', 'name'),
          operator: 'notIn',
          paramIndex: 0,
          columnType: 'string',
        }
        check(base({ where: cond }), [['a', 'b']], cfg.notInString)
      })

      it('in with int type', () => {
        const cond: WhereCondition = { column: col('t0', 'age'), operator: 'in', paramIndex: 0, columnType: 'int' }
        check(base({ where: cond }), [[1, 2]], cfg.inInt)
      })

      it('in defaults when type unknown', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'in', paramIndex: 0 }
        check(base({ where: cond }), [['a']], cfg.inDefaultType)
      })

      it('in single element', () => {
        const cond: WhereCondition = { column: col('t0', 'id'), operator: 'in', paramIndex: 0, columnType: 'uuid' }
        check(base({ where: cond }), [['only']], cfg.inSingleElement)
      })
    })

    // ── Type casts (via IN) ───────────────────────────────

    describe('type casts', () => {
      it('decimal', () => {
        const cond: WhereCondition = {
          column: col('t0', 'price'),
          operator: 'in',
          paramIndex: 0,
          columnType: 'decimal',
        }
        check(base({ where: cond }), [[1.5]], cfg.typeCastDecimal)
      })
    })

    // ── Float param ───────────────────────────────────────

    describe('float param', () => {
      it('float → dialect-specific type', () => {
        const cond: WhereCondition = { column: col('t0', 'score'), operator: '>', paramIndex: 0 }
        check(base({ where: cond }), [3.14], cfg.floatParam)
      })
    })

    // ── Catalog-qualified ─────────────────────────────────

    describe('catalog-qualified', () => {
      it('catalog-qualified table', () => {
        check(base({ from: tbl(`${cfg.schema}.users`, 't0', 'pg_main') }), [], cfg.catalogTable)
      })

      it('catalog-qualified join', () => {
        const parts = base({
          joins: [
            {
              type: 'inner',
              table: tbl(`${sub}.orders`, 't1', 'pg_main'),
              leftColumn: col('t0', 'id'),
              rightColumn: col('t1', 'user_id'),
            },
          ],
        })
        check(parts, [], cfg.catalogJoin)
      })
    })

    // ── HAVING (extended) ─────────────────────────────────

    describe('HAVING (extended)', () => {
      it('having AND group (#72)', () => {
        const parts = base({
          select: [col('t0', 'status')],
          from: tbl(`${sub}.orders`, 't0'),
          groupBy: [col('t0', 'status')],
          aggregations: [
            { fn: 'sum', column: col('t0', 'total'), alias: 'totalSum' },
            { fn: 'count', column: '*', alias: 'cnt' },
          ],
          having: {
            logic: 'and',
            conditions: [
              { column: 'totalSum', operator: '>', paramIndex: 0 },
              { column: 'cnt', operator: '>', paramIndex: 1 },
            ],
          } satisfies HavingGroup,
        })
        check(parts, [100, 5], cfg.havingAndGroup)
      })

      it('having OR group (#73)', () => {
        const parts = base({
          select: [col('t0', 'status')],
          from: tbl(`${sub}.orders`, 't0'),
          groupBy: [col('t0', 'status')],
          aggregations: [
            { fn: 'sum', column: col('t0', 'total'), alias: 'totalSum' },
            { fn: 'avg', column: col('t0', 'total'), alias: 'avgTotal' },
          ],
          having: {
            logic: 'or',
            conditions: [
              { column: 'totalSum', operator: '>', paramIndex: 0 },
              { column: 'avgTotal', operator: '>', paramIndex: 1 },
            ],
          } satisfies HavingGroup,
        })
        check(parts, [1000, 200], cfg.havingOrGroup)
      })

      it('having NOT group (#144)', () => {
        const parts = base({
          select: [col('t0', 'status')],
          from: tbl(`${sub}.orders`, 't0'),
          groupBy: [col('t0', 'status')],
          aggregations: [
            { fn: 'sum', column: col('t0', 'total'), alias: 'totalSum' },
            { fn: 'count', column: '*', alias: 'cnt' },
          ],
          having: {
            logic: 'or',
            not: true,
            conditions: [
              { column: 'totalSum', operator: '>', paramIndex: 0 },
              { column: 'cnt', operator: '>', paramIndex: 1 },
            ],
          } satisfies HavingGroup,
        })
        check(parts, [100, 5], cfg.havingNotGroup)
      })

      it('having isNull (#197)', () => {
        const parts = base({
          select: [col('t0', 'status')],
          from: tbl(`${sub}.orders`, 't0'),
          groupBy: [col('t0', 'status')],
          aggregations: [{ fn: 'sum', column: col('t0', 'discount'), alias: 'discountSum' }],
          having: { column: 'discountSum', operator: 'isNull' },
        })
        check(parts, [], cfg.havingIsNull)
      })
    })

    // ── Complex WHERE ─────────────────────────────────────

    describe('complex WHERE', () => {
      it('EXISTS inside OR group (#27)', () => {
        const cond: WhereGroup = {
          logic: 'or',
          conditions: [
            { column: col('t0', 'status'), operator: '=', paramIndex: 0 } as WhereCondition,
            {
              exists: true,
              subquery: {
                from: tbl(`${sub}.orders`, 's0'),
                join: { leftColumn: col('t0', 'id'), rightColumn: col('s0', 'user_id') },
              },
            } as WhereExists,
          ],
        }
        check(base({ where: cond }), ['active'], cfg.existsInsideOrGroup)
      })

      it('deeply nested WHERE (#70)', () => {
        const cond: WhereGroup = {
          logic: 'or',
          conditions: [
            { column: col('t0', 'status'), operator: '=', paramIndex: 0 } as WhereCondition,
            {
              logic: 'and',
              conditions: [
                { column: col('t0', 'age'), operator: '>', paramIndex: 1 } as WhereCondition,
                {
                  logic: 'or',
                  conditions: [
                    { column: col('t0', 'name'), operator: '=', paramIndex: 2 } as WhereCondition,
                    { column: col('t0', 'name'), operator: '=', paramIndex: 3 } as WhereCondition,
                  ],
                } as WhereGroup,
              ],
            } as WhereGroup,
          ],
        }
        check(base({ where: cond }), ['active', 18, 'Alice', 'Bob'], cfg.deeplyNestedWhere)
      })

      it('mixed filter + group + exists (#71)', () => {
        const cond: WhereGroup = {
          logic: 'and',
          conditions: [
            { column: col('t0', 'status'), operator: '=', paramIndex: 0 } as WhereCondition,
            {
              logic: 'or',
              conditions: [
                { column: col('t0', 'age'), operator: '>', paramIndex: 1 } as WhereCondition,
                { column: col('t0', 'age'), operator: '<', paramIndex: 2 } as WhereCondition,
              ],
            } as WhereGroup,
            {
              exists: true,
              subquery: {
                from: tbl(`${sub}.orders`, 's0'),
                join: { leftColumn: col('t0', 'id'), rightColumn: col('s0', 'user_id') },
              },
            } as WhereExists,
          ],
        }
        check(base({ where: cond }), ['active', 65, 18], cfg.mixedFilterGroupExists)
      })

      it('counted subquery with sub-filters (#161)', () => {
        const subWhere: WhereCondition = { column: col('s0', 'status'), operator: '=', paramIndex: 1 }
        const cond: WhereCountedSubquery = {
          subquery: {
            from: tbl(`${sub}.orders`, 's0'),
            join: { leftColumn: col('t0', 'id'), rightColumn: col('s0', 'user_id') },
            where: subWhere,
          },
          operator: '=',
          countParamIndex: 0,
        }
        check(base({ where: cond }), [2, 'paid'], cfg.countedWithSubFilters)
      })
    })

    // ── Nested EXISTS ─────────────────────────────────────

    describe('nested EXISTS', () => {
      it('nested EXISTS (#227)', () => {
        const innerExists: WhereExists = {
          exists: true,
          subquery: {
            from: tbl(`${sub}.tenants`, 's1'),
            join: { leftColumn: col('s0', 'tenant_id'), rightColumn: col('s1', 'id') },
          },
        }
        const outerExists: WhereExists = {
          exists: true,
          subquery: {
            from: tbl(`${sub}.invoices`, 's0'),
            join: { leftColumn: col('t0', 'id'), rightColumn: col('s0', 'order_id') },
            where: innerExists,
          },
        }
        check(base({ where: outerExists }), [], cfg.nestedExists)
      })
    })

    // ── Join-related ──────────────────────────────────────

    describe('join-related', () => {
      it('filter on joined column (#69/#138)', () => {
        const parts = base({
          joins: [
            {
              type: 'left',
              table: tbl(`${sub}.products`, 't1'),
              leftColumn: col('t0', 'product_id'),
              rightColumn: col('t1', 'id'),
            },
          ],
          where: { column: col('t1', 'category'), operator: '=', paramIndex: 0 } as WhereCondition,
        })
        check(parts, ['electronics'], cfg.filterOnJoinedColumn)
      })

      it('3-table JOIN (#127)', () => {
        const parts = base({
          joins: [
            {
              type: 'left',
              table: tbl(`${sub}.orders`, 't1'),
              leftColumn: col('t0', 'id'),
              rightColumn: col('t1', 'user_id'),
            },
            {
              type: 'inner',
              table: tbl(`${sub}.products`, 't2'),
              leftColumn: col('t1', 'product_id'),
              rightColumn: col('t2', 'id'),
            },
          ],
        })
        check(parts, [], cfg.threeTableJoin)
      })

      it('multi-join with per-table filters (#147)', () => {
        const parts = base({
          joins: [
            {
              type: 'left',
              table: tbl(`${sub}.products`, 't1'),
              leftColumn: col('t0', 'product_id'),
              rightColumn: col('t1', 'id'),
            },
            {
              type: 'left',
              table: tbl(`${sub}.categories`, 't2'),
              leftColumn: col('t1', 'category_id'),
              rightColumn: col('t2', 'id'),
            },
          ],
          where: {
            logic: 'and',
            conditions: [
              { column: col('t1', 'active'), operator: '=', paramIndex: 0 } as WhereCondition,
              { column: col('t2', 'name'), operator: '=', paramIndex: 1 } as WhereCondition,
            ],
          } as WhereGroup,
        })
        check(parts, [true, 'electronics'], cfg.multiJoinPerTableFilters)
      })

      it('aggregation on joined column (#126)', () => {
        const parts = base({
          select: [col('t0', 'status')],
          from: tbl(`${sub}.orders`, 't0'),
          joins: [
            {
              type: 'inner',
              table: tbl(`${sub}.products`, 't1'),
              leftColumn: col('t0', 'product_id'),
              rightColumn: col('t1', 'id'),
            },
          ],
          groupBy: [col('t0', 'status')],
          aggregations: [{ fn: 'sum', column: col('t1', 'price'), alias: 'totalPrice' }],
        })
        check(parts, [], cfg.aggOnJoinedColumn)
      })
    })

    // ── Cross-table ORDER BY ──────────────────────────────

    describe('cross-table ORDER BY', () => {
      it('order by joined column (#24)', () => {
        const parts = base({
          joins: [
            {
              type: 'left',
              table: tbl(`${sub}.orders`, 't1'),
              leftColumn: col('t0', 'id'),
              rightColumn: col('t1', 'user_id'),
            },
          ],
          orderBy: [{ column: col('t1', 'created_at'), direction: 'desc' }],
        })
        check(parts, [], cfg.crossTableOrderBy)
      })
    })

    // ── Array ops on int[] ────────────────────────────────

    describe('array ops on int[]', () => {
      it('arrayContains on int[] (#203)', () => {
        const cond: WhereArrayCondition = {
          column: col('t0', 'priorities'),
          operator: 'contains',
          paramIndexes: [0],
          elementType: 'int',
        }
        check(base({ where: cond }), [1], cfg.arrayContainsInt)
      })

      it('arrayContainsAll on int[] (#204)', () => {
        const cond: WhereArrayCondition = {
          column: col('t0', 'priorities'),
          operator: 'containsAll',
          paramIndexes: [0],
          elementType: 'int',
        }
        check(base({ where: cond }), [[1, 2, 3]], cfg.arrayContainsAllInt)
      })

      it('array filter in AND group (#205)', () => {
        const cond: WhereGroup = {
          logic: 'and',
          conditions: [
            {
              column: col('t0', 'tags'),
              operator: 'containsAny',
              paramIndexes: [0],
              elementType: 'string',
            } as WhereArrayCondition,
            { column: col('t0', 'price'), operator: '>', paramIndex: 1 } as WhereCondition,
          ],
        }
        check(base({ where: cond }), [['sale'], 10], cfg.arrayInGroup)
      })

      it('array filter on joined table (#206)', () => {
        const parts = base({
          joins: [
            {
              type: 'left',
              table: tbl(`${sub}.products`, 't1'),
              leftColumn: col('t0', 'product_id'),
              rightColumn: col('t1', 'id'),
            },
          ],
          where: {
            column: col('t1', 'labels'),
            operator: 'containsAny',
            paramIndexes: [0],
            elementType: 'string',
          } as WhereArrayCondition,
        })
        check(parts, [['sale']], cfg.arrayOnJoinedTable)
      })

      it('arrayContainsAll single element (#207)', () => {
        const cond: WhereArrayCondition = {
          column: col('t0', 'tags'),
          operator: 'containsAll',
          paramIndexes: [0],
          elementType: 'string',
        }
        check(base({ where: cond }), [['sale']], cfg.arrayContainsAllSingleElement)
      })
    })

    // ── distinct + groupBy ────────────────────────────────

    describe('distinct + groupBy', () => {
      it('distinct with groupBy (#163)', () => {
        const parts = base({
          select: [col('t0', 'status')],
          distinct: true,
          from: tbl(`${sub}.orders`, 't0'),
          groupBy: [col('t0', 'status')],
          aggregations: [{ fn: 'sum', column: col('t0', 'total'), alias: 'totalSum' }],
        })
        check(parts, [], cfg.distinctGroupBy)
      })
    })

    // ── Full query ────────────────────────────────────────

    describe('full query', () => {
      it('complex query combines all clauses', () => {
        const parts: SqlParts = {
          select: [col('t0', 'status')],
          distinct: true,
          from: tbl(`${sub}.orders`, 't0'),
          joins: [
            {
              type: 'inner',
              table: tbl(`${sub}.users`, 't1'),
              leftColumn: col('t0', 'user_id'),
              rightColumn: col('t1', 'id'),
            },
          ],
          where: { column: col('t1', 'age'), operator: '>=', paramIndex: 0 },
          groupBy: [col('t0', 'status')],
          having: { column: 'cnt', operator: '>', paramIndex: 1 },
          aggregations: [{ fn: 'count', column: '*', alias: 'cnt' }],
          orderBy: [{ column: 'cnt', direction: 'desc' }],
          limit: 5,
          offset: 0,
        }
        check(parts, [18, 2], cfg.fullQuery)
      })

      it('param order preserved', () => {
        const where: WhereGroup = {
          logic: 'and',
          conditions: [
            { column: col('t0', 'name'), operator: '=', paramIndex: 0 } as WhereCondition,
            { column: col('t0', 'age'), operator: '>=', paramIndex: 1 } as WhereCondition,
            { column: col('t0', 'age'), operator: '<=', paramIndex: 2 } as WhereCondition,
          ],
        }
        check(base({ where }), ['Alice', 18, 65], cfg.paramOrder)
      })
    })

    // ── Injection defense-in-depth ─────────────────────────

    describe('Injection defense-in-depth', () => {
      it('aggregation alias escapes quote chars', () => {
        const parts = base({
          select: [col('t0', 'status')],
          from: tbl(`${sub}.orders`, 't0'),
          groupBy: [col('t0', 'status')],
          aggregations: [{ fn: 'sum', column: col('t0', 'total'), alias: cfg.injectionAggAlias.sql[0] as string }],
        })
        const result = dialect.generate(parts, [])
        for (const s of cfg.injectionAggAlias.notSql ?? []) expect(result.sql).not.toContain(s)
      })

      it('ORDER BY alias escapes quote chars', () => {
        const parts = base({
          select: [col('t0', 'status')],
          from: tbl(`${sub}.orders`, 't0'),
          groupBy: [col('t0', 'status')],
          aggregations: [{ fn: 'count', column: '*', alias: 'cnt' }],
          orderBy: [{ column: cfg.injectionOrderByAlias.sql[0] as string, direction: 'asc' }],
        })
        const result = dialect.generate(parts, [])
        for (const s of cfg.injectionOrderByAlias.notSql ?? []) expect(result.sql).not.toContain(s)
      })

      it('HAVING alias escapes quote chars', () => {
        const parts = base({
          select: [col('t0', 'status')],
          from: tbl(`${sub}.orders`, 't0'),
          groupBy: [col('t0', 'status')],
          aggregations: [{ fn: 'sum', column: col('t0', 'total'), alias: 'total' }],
          having: { alias: cfg.injectionHavingAlias.sql[0] as string, fromParamIndex: 0, toParamIndex: 1 },
        })
        const result = dialect.generate(parts, [100, 1000])
        for (const s of cfg.injectionHavingAlias.notSql ?? []) expect(result.sql).not.toContain(s)
      })

      it('safeAggFn rejects malicious function name', () => {
        const parts = base({
          select: [col('t0', 'status')],
          from: tbl(`${sub}.orders`, 't0'),
          groupBy: [col('t0', 'status')],
          aggregations: [
            { fn: 'sum); DROP TABLE orders;--' as AggregationClause['fn'], column: col('t0', 'total'), alias: 'x' },
          ],
        })
        check(parts, [], cfg.injectionSafeAggFn)
      })

      it('WHERE string column escapes quote chars', () => {
        const cond: WhereCondition = { column: cfg.injectionWhereStringColumn.sql[0] as string, operator: '>', paramIndex: 0 }
        const result = dialect.generate(base({ where: cond }), [0])
        for (const s of cfg.injectionWhereStringColumn.notSql ?? []) expect(result.sql).not.toContain(s)
      })
    })
  })
}
