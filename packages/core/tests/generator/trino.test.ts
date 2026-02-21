import { describe, expect, it } from 'vitest'
import { TrinoDialect } from '../../src/dialects/trino.js'
import type {
  ColumnRef,
  SqlParts,
  TableRef,
  WhereArrayCondition,
  WhereBetween,
  WhereColumnCondition,
  WhereCondition,
  WhereCountedSubquery,
  WhereExists,
  WhereFunction,
  WhereGroup,
} from '../../src/types/ir.js'

const dialect = new TrinoDialect()

function col(table: string, name: string): ColumnRef {
  return { tableAlias: table, columnName: name }
}

function tbl(physical: string, alias: string, catalog?: string): TableRef {
  const ref: TableRef = { physicalName: physical, alias }
  if (catalog !== undefined) {
    ref.catalog = catalog
  }
  return ref
}

function base(overrides: Partial<SqlParts> = {}): SqlParts {
  return {
    select: [col('t0', 'id'), col('t0', 'name')],
    from: tbl('public.users', 't0'),
    joins: [],
    groupBy: [],
    aggregations: [],
    orderBy: [],
    ...overrides,
  }
}

describe('Trino — SELECT & FROM', () => {
  it('double-quote quoting', () => {
    const { sql } = dialect.generate(base(), [])
    expect(sql).toBe('SELECT "t0"."id" AS "t0__id", "t0"."name" AS "t0__name" FROM "public"."users" AS "t0"')
  })

  it('catalog-qualified table', () => {
    const { sql } = dialect.generate(base({ from: tbl('public.users', 't0', 'pg_main') }), [])
    expect(sql).toContain('FROM "pg_main"."public"."users" AS "t0"')
  })

  it('count mode', () => {
    const { sql } = dialect.generate(base({ select: [], countMode: true }), [])
    expect(sql).toBe('SELECT COUNT(*) FROM "public"."users" AS "t0"')
  })

  it('distinct', () => {
    const { sql } = dialect.generate(base({ distinct: true }), [])
    expect(sql).toContain('SELECT DISTINCT')
  })
})

describe('Trino — positional ? params', () => {
  it('single param', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: '=', paramIndex: 0 }
    const { sql, params } = dialect.generate(base({ where: cond }), ['Alice'])
    expect(sql).toContain('WHERE "t0"."name" = ?')
    expect(params).toEqual(['Alice'])
  })

  it('multiple params in order', () => {
    const cond: WhereGroup = {
      logic: 'and',
      conditions: [
        { column: col('t0', 'name'), operator: '=', paramIndex: 0 } as WhereCondition,
        { column: col('t0', 'age'), operator: '>=', paramIndex: 1 } as WhereCondition,
      ],
    }
    const { sql, params } = dialect.generate(base({ where: cond }), ['Alice', 18])
    expect(sql).toContain('("t0"."name" = ? AND "t0"."age" >= ?)')
    expect(params).toEqual(['Alice', 18])
  })
})

describe('Trino — IN / NOT IN (expanded)', () => {
  it('in expands to (?, ?, ...)', () => {
    const cond: WhereCondition = { column: col('t0', 'id'), operator: 'in', paramIndex: 0, columnType: 'uuid' }
    const { sql, params } = dialect.generate(base({ where: cond }), [['id1', 'id2', 'id3']])
    expect(sql).toContain('"t0"."id" IN (?, ?, ?)')
    expect(params).toEqual(['id1', 'id2', 'id3'])
  })

  it('notIn expands', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'notIn', paramIndex: 0, columnType: 'string' }
    const { sql, params } = dialect.generate(base({ where: cond }), [['a', 'b']])
    expect(sql).toContain('"t0"."name" NOT IN (?, ?)')
    expect(params).toEqual(['a', 'b'])
  })

  it('in single element', () => {
    const cond: WhereCondition = { column: col('t0', 'id'), operator: 'in', paramIndex: 0, columnType: 'uuid' }
    const { sql, params } = dialect.generate(base({ where: cond }), [['only']])
    expect(sql).toContain('"t0"."id" IN (?)')
    expect(params).toEqual(['only'])
  })
})

describe('Trino — NULL checks', () => {
  it('IS NULL', () => {
    const cond: WhereCondition = { column: col('t0', 'age'), operator: 'isNull' }
    const { sql } = dialect.generate(base({ where: cond }), [])
    expect(sql).toContain('"t0"."age" IS NULL')
  })
})

describe('Trino — case-insensitive via lower()', () => {
  it('ilike → lower() LIKE lower()', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'ilike', paramIndex: 0 }
    const { sql } = dialect.generate(base({ where: cond }), ['%foo%'])
    expect(sql).toContain('lower("t0"."name") LIKE lower(?)')
  })

  it('notIlike → NOT (lower(...) LIKE lower(...))', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'notIlike', paramIndex: 0 }
    const { sql } = dialect.generate(base({ where: cond }), ['%x%'])
    expect(sql).toContain('NOT (lower("t0"."name") LIKE lower(?))')
  })

  it('istartsWith', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'istartsWith', paramIndex: 0 }
    const { sql, params } = dialect.generate(base({ where: cond }), ['ali'])
    expect(sql).toContain('lower("t0"."name") LIKE lower(?) ESCAPE \'\\\'')
    expect(params).toEqual(['ali%'])
  })

  it('iendsWith', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'iendsWith', paramIndex: 0 }
    const { sql, params } = dialect.generate(base({ where: cond }), ['ICE'])
    expect(sql).toContain('lower("t0"."name") LIKE lower(?) ESCAPE \'\\\'')
    expect(params).toEqual(['%ICE'])
  })

  it('icontains', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'icontains', paramIndex: 0 }
    const { sql, params } = dialect.generate(base({ where: cond }), ['LIC'])
    expect(sql).toContain('lower("t0"."name") LIKE lower(?) ESCAPE \'\\\'')
    expect(params).toEqual(['%LIC%'])
  })

  it('notIcontains', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'notIcontains', paramIndex: 0 }
    const { sql, params } = dialect.generate(base({ where: cond }), ['BAD'])
    expect(sql).toContain('NOT (lower("t0"."name") LIKE lower(?) ESCAPE \'\\\')')
    expect(params).toEqual(['%BAD%'])
  })
})

describe('Trino — pattern wrapping', () => {
  it('startsWith with ESCAPE', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'startsWith', paramIndex: 0 }
    const { sql, params } = dialect.generate(base({ where: cond }), ['Ali'])
    expect(sql).toContain('"t0"."name" LIKE ? ESCAPE \'\\\'')
    expect(params).toEqual(['Ali%'])
  })

  it('endsWith', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'endsWith', paramIndex: 0 }
    const { params } = dialect.generate(base({ where: cond }), ['ice'])
    expect(params).toEqual(['%ice'])
  })

  it('contains', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'contains', paramIndex: 0 }
    const { params } = dialect.generate(base({ where: cond }), ['lic'])
    expect(params).toEqual(['%lic%'])
  })

  it('notContains', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'notContains', paramIndex: 0 }
    const { sql } = dialect.generate(base({ where: cond }), ['bad'])
    expect(sql).toContain('"t0"."name" NOT LIKE ? ESCAPE \'\\\'')
  })
})

describe('Trino — BETWEEN', () => {
  it('between', () => {
    const cond: WhereBetween = { column: col('t0', 'age'), fromParamIndex: 0, toParamIndex: 1 }
    const { sql } = dialect.generate(base({ where: cond }), [18, 65])
    expect(sql).toContain('"t0"."age" BETWEEN ? AND ?')
  })

  it('notBetween', () => {
    const cond: WhereBetween = { column: col('t0', 'age'), not: true, fromParamIndex: 0, toParamIndex: 1 }
    const { sql } = dialect.generate(base({ where: cond }), [0, 17])
    expect(sql).toContain('"t0"."age" NOT BETWEEN ? AND ?')
  })
})

describe('Trino — levenshtein_distance', () => {
  it('levenshtein → levenshtein_distance', () => {
    const cond: WhereFunction = {
      fn: 'levenshtein',
      column: col('t0', 'name'),
      fnParamIndex: 0,
      operator: '<=',
      compareParamIndex: 1,
    }
    const { sql } = dialect.generate(base({ where: cond }), ['test', 2])
    expect(sql).toContain('levenshtein_distance("t0"."name", ?) <= ?')
  })
})

describe('Trino — array operators', () => {
  it('contains()', () => {
    const cond: WhereArrayCondition = {
      column: col('t0', 'tags'),
      operator: 'contains',
      paramIndexes: [0],
      elementType: 'string',
    }
    const { sql } = dialect.generate(base({ where: cond }), ['urgent'])
    expect(sql).toContain('contains("t0"."tags", ?)')
  })

  it('containsAll via array_except', () => {
    const cond: WhereArrayCondition = {
      column: col('t0', 'tags'),
      operator: 'containsAll',
      paramIndexes: [0],
      elementType: 'string',
    }
    const { sql } = dialect.generate(base({ where: cond }), [['a', 'b']])
    expect(sql).toContain('cardinality(array_except(ARRAY[?, ?], "t0"."tags")) = 0')
  })

  it('containsAny via arrays_overlap', () => {
    const cond: WhereArrayCondition = {
      column: col('t0', 'tags'),
      operator: 'containsAny',
      paramIndexes: [0],
      elementType: 'string',
    }
    const { sql } = dialect.generate(base({ where: cond }), [['x', 'y']])
    expect(sql).toContain('arrays_overlap("t0"."tags", ARRAY[?, ?])')
  })

  it('isEmpty via cardinality', () => {
    const cond: WhereArrayCondition = { column: col('t0', 'tags'), operator: 'isEmpty', elementType: 'string' }
    const { sql } = dialect.generate(base({ where: cond }), [])
    expect(sql).toContain('cardinality("t0"."tags") = 0')
  })

  it('isNotEmpty via cardinality', () => {
    const cond: WhereArrayCondition = { column: col('t0', 'tags'), operator: 'isNotEmpty', elementType: 'string' }
    const { sql } = dialect.generate(base({ where: cond }), [])
    expect(sql).toContain('cardinality("t0"."tags") > 0')
  })
})

describe('Trino — column comparison', () => {
  it('column-to-column', () => {
    const cond: WhereColumnCondition = { leftColumn: col('t0', 'a'), operator: '>', rightColumn: col('t1', 'b') }
    const { sql } = dialect.generate(base({ where: cond }), [])
    expect(sql).toContain('"t0"."a" > "t1"."b"')
  })
})

describe('Trino — EXISTS', () => {
  it('EXISTS subquery', () => {
    const cond: WhereExists = {
      exists: true,
      subquery: {
        from: tbl('public.orders', 's0'),
        join: { leftColumn: col('t0', 'id'), rightColumn: col('s0', 'user_id') },
      },
    }
    const { sql } = dialect.generate(base({ where: cond }), [])
    expect(sql).toContain('EXISTS (SELECT 1 FROM "public"."orders" AS "s0" WHERE "t0"."id" = "s0"."user_id")')
  })

  it('NOT EXISTS', () => {
    const cond: WhereExists = {
      exists: false,
      subquery: {
        from: tbl('public.orders', 's0'),
        join: { leftColumn: col('t0', 'id'), rightColumn: col('s0', 'user_id') },
      },
    }
    const { sql } = dialect.generate(base({ where: cond }), [])
    expect(sql).toContain('NOT EXISTS')
  })
})

describe('Trino — counted subquery', () => {
  it('counted >=', () => {
    const cond: WhereCountedSubquery = {
      subquery: {
        from: tbl('public.orders', 's0'),
        join: { leftColumn: col('t0', 'id'), rightColumn: col('s0', 'user_id') },
      },
      operator: '>=',
      countParamIndex: 0,
    }
    const { sql, params } = dialect.generate(base({ where: cond }), [5])
    expect(sql).toContain(
      '(SELECT COUNT(*) FROM (SELECT 1 FROM "public"."orders" AS "s0" WHERE "t0"."id" = "s0"."user_id" LIMIT 5) AS "_c") >= ?',
    )
    expect(params).toEqual([5])
  })
})

describe('Trino — GROUP BY + agg', () => {
  it('group by with count', () => {
    const parts = base({
      select: [col('t0', 'status')],
      groupBy: [col('t0', 'status')],
      aggregations: [{ fn: 'count', column: '*', alias: 'cnt' }],
    })
    const { sql } = dialect.generate(parts, [])
    expect(sql).toContain('COUNT(*) AS "cnt"')
    expect(sql).toContain('GROUP BY "t0"."status"')
  })
})

describe('Trino — ORDER BY / LIMIT', () => {
  it('order by + limit + offset', () => {
    const parts = base({
      orderBy: [{ column: col('t0', 'name'), direction: 'asc' }],
      limit: 10,
      offset: 20,
    })
    const { sql } = dialect.generate(parts, [])
    expect(sql).toContain('ORDER BY "t0"."name" ASC LIMIT 10 OFFSET 20')
  })

  it('order by aggregation alias', () => {
    const parts = base({
      select: [col('t0', 'status')],
      groupBy: [col('t0', 'status')],
      aggregations: [{ fn: 'count', column: '*', alias: 'cnt' }],
      orderBy: [{ column: 'cnt', direction: 'desc' }],
    })
    const { sql } = dialect.generate(parts, [])
    expect(sql).toContain('ORDER BY "cnt" DESC')
  })
})

describe('Trino — JOIN', () => {
  it('inner join', () => {
    const parts = base({
      joins: [
        {
          type: 'inner',
          table: tbl('public.orders', 't1'),
          leftColumn: col('t0', 'id'),
          rightColumn: col('t1', 'user_id'),
        },
      ],
    })
    const { sql } = dialect.generate(parts, [])
    expect(sql).toContain('INNER JOIN "public"."orders" AS "t1" ON "t0"."id" = "t1"."user_id"')
  })

  it('join with catalog', () => {
    const parts = base({
      joins: [
        {
          type: 'inner',
          table: tbl('public.orders', 't1', 'pg_main'),
          leftColumn: col('t0', 'id'),
          rightColumn: col('t1', 'user_id'),
        },
      ],
    })
    const { sql } = dialect.generate(parts, [])
    expect(sql).toContain('INNER JOIN "pg_main"."public"."orders" AS "t1"')
  })
})

describe('Trino — HAVING', () => {
  it('having', () => {
    const parts = base({
      select: [col('t0', 'status')],
      groupBy: [col('t0', 'status')],
      aggregations: [{ fn: 'count', column: '*', alias: 'cnt' }],
      having: { column: 'cnt', operator: '>', paramIndex: 0 },
    })
    const { sql, params } = dialect.generate(parts, [5])
    expect(sql).toContain('HAVING "cnt" > ?')
    expect(params).toEqual([5])
  })
})

describe('Trino — SELECT additional', () => {
  it('empty select falls back to *', () => {
    const { sql } = dialect.generate(base({ select: [] }), [])
    expect(sql).toBe('SELECT * FROM "public"."users" AS "t0"')
  })
})

describe('Trino — JOIN additional', () => {
  it('left join', () => {
    const parts = base({
      joins: [
        {
          type: 'left',
          table: tbl('public.orders', 't1'),
          leftColumn: col('t0', 'id'),
          rightColumn: col('t1', 'user_id'),
        },
      ],
    })
    const { sql } = dialect.generate(parts, [])
    expect(sql).toContain('LEFT JOIN "public"."orders" AS "t1" ON "t0"."id" = "t1"."user_id"')
  })
})

describe('Trino — WHERE standard additional', () => {
  it('not equals', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: '!=', paramIndex: 0 }
    const { sql } = dialect.generate(base({ where: cond }), ['Alice'])
    expect(sql).toContain('WHERE "t0"."name" != ?')
  })

  it('less than', () => {
    const cond: WhereCondition = { column: col('t0', 'age'), operator: '<', paramIndex: 0 }
    const { sql } = dialect.generate(base({ where: cond }), [30])
    expect(sql).toContain('WHERE "t0"."age" < ?')
  })

  it('greater than or equal', () => {
    const cond: WhereCondition = { column: col('t0', 'age'), operator: '>=', paramIndex: 0 }
    const { sql } = dialect.generate(base({ where: cond }), [18])
    expect(sql).toContain('WHERE "t0"."age" >= ?')
  })

  it('IS NOT NULL', () => {
    const cond: WhereCondition = { column: col('t0', 'age'), operator: 'isNotNull' }
    const { sql } = dialect.generate(base({ where: cond }), [])
    expect(sql).toContain('"t0"."age" IS NOT NULL')
  })
})

describe('Trino — LIKE additional', () => {
  it('like raw pattern', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'like', paramIndex: 0 }
    const { sql, params } = dialect.generate(base({ where: cond }), ['%Alice%'])
    expect(sql).toContain('WHERE "t0"."name" LIKE ?')
    expect(params).toEqual(['%Alice%'])
  })

  it('notLike', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'notLike', paramIndex: 0 }
    const { sql } = dialect.generate(base({ where: cond }), ['%x%'])
    expect(sql).toContain('WHERE "t0"."name" NOT LIKE ?')
  })
})

describe('Trino — wildcard escaping', () => {
  it('escapes % in startsWith', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'startsWith', paramIndex: 0 }
    const { params } = dialect.generate(base({ where: cond }), ['100%'])
    expect(params).toEqual(['100\\%%'])
  })

  it('escapes _ in contains', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'contains', paramIndex: 0 }
    const { params } = dialect.generate(base({ where: cond }), ['a_b'])
    expect(params).toEqual(['%a\\_b%'])
  })

  it('escapes backslash in endsWith', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'endsWith', paramIndex: 0 }
    const { params } = dialect.generate(base({ where: cond }), ['a\\b'])
    expect(params).toEqual(['%a\\\\b'])
  })
})

describe('Trino — groups additional', () => {
  it('OR group', () => {
    const cond: WhereGroup = {
      logic: 'or',
      conditions: [
        { column: col('t0', 'name'), operator: '=', paramIndex: 0 } as WhereCondition,
        { column: col('t0', 'name'), operator: '=', paramIndex: 1 } as WhereCondition,
      ],
    }
    const { sql, params } = dialect.generate(base({ where: cond }), ['Alice', 'Bob'])
    expect(sql).toContain('WHERE ("t0"."name" = ? OR "t0"."name" = ?)')
    expect(params).toEqual(['Alice', 'Bob'])
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
    const { sql } = dialect.generate(base({ where: cond }), ['x', 'y'])
    expect(sql).toContain('WHERE NOT ("t0"."name" = ? OR "t0"."name" = ?)')
  })

  it('single-element group — no parens', () => {
    const cond: WhereGroup = {
      logic: 'and',
      conditions: [{ column: col('t0', 'age'), operator: '>', paramIndex: 0 } as WhereCondition],
    }
    const { sql } = dialect.generate(base({ where: cond }), [10])
    expect(sql).toContain('WHERE "t0"."age" > ?')
    expect(sql).not.toContain('("t0"."age"')
  })
})

describe('Trino — EXISTS additional', () => {
  it('EXISTS with sub-filters', () => {
    const subWhere: WhereCondition = { column: col('s0', 'status'), operator: '=', paramIndex: 0 }
    const cond: WhereExists = {
      exists: true,
      subquery: {
        from: tbl('public.orders', 's0'),
        join: { leftColumn: col('t0', 'id'), rightColumn: col('s0', 'user_id') },
        where: subWhere,
      },
    }
    const { sql, params } = dialect.generate(base({ where: cond }), ['active'])
    expect(sql).toContain('WHERE "t0"."id" = "s0"."user_id" AND "s0"."status" = ?')
    expect(params).toEqual(['active'])
  })
})

describe('Trino — aggregations additional', () => {
  it('sum aggregation', () => {
    const parts = base({
      select: [col('t0', 'status')],
      from: tbl('public.orders', 't0'),
      groupBy: [col('t0', 'status')],
      aggregations: [{ fn: 'sum', column: col('t0', 'total'), alias: 'total' }],
    })
    const { sql } = dialect.generate(parts, [])
    expect(sql).toContain('SUM("t0"."total") AS "total"')
  })

  it('avg, min, max', () => {
    const parts = base({
      select: [],
      from: tbl('public.orders', 't0'),
      aggregations: [
        { fn: 'avg', column: col('t0', 'total'), alias: 'avg_amount' },
        { fn: 'min', column: col('t0', 'total'), alias: 'min_amount' },
        { fn: 'max', column: col('t0', 'total'), alias: 'max_amount' },
      ],
    })
    const { sql } = dialect.generate(parts, [])
    expect(sql).toContain('AVG("t0"."total") AS "avg_amount"')
    expect(sql).toContain('MIN("t0"."total") AS "min_amount"')
    expect(sql).toContain('MAX("t0"."total") AS "max_amount"')
  })
})

describe('Trino — HAVING additional', () => {
  it('having between', () => {
    const parts = base({
      select: [col('t0', 'status')],
      from: tbl('public.orders', 't0'),
      groupBy: [col('t0', 'status')],
      aggregations: [{ fn: 'sum', column: col('t0', 'total'), alias: 'total' }],
      having: { alias: 'total', fromParamIndex: 0, toParamIndex: 1 },
    })
    const { sql, params } = dialect.generate(parts, [100, 1000])
    expect(sql).toContain('HAVING "total" BETWEEN ? AND ?')
    expect(params).toEqual([100, 1000])
  })

  it('having not between', () => {
    const parts = base({
      select: [col('t0', 'status')],
      from: tbl('public.orders', 't0'),
      groupBy: [col('t0', 'status')],
      aggregations: [{ fn: 'sum', column: col('t0', 'total'), alias: 'total' }],
      having: { alias: 'total', not: true, fromParamIndex: 0, toParamIndex: 1 },
    })
    const { sql, params } = dialect.generate(parts, [100, 1000])
    expect(sql).toContain('HAVING "total" NOT BETWEEN ? AND ?')
    expect(params).toEqual([100, 1000])
  })
})

describe('Trino — ORDER BY additional', () => {
  it('order by column asc', () => {
    const parts = base({ orderBy: [{ column: col('t0', 'name'), direction: 'asc' }] })
    const { sql } = dialect.generate(parts, [])
    expect(sql).toContain('ORDER BY "t0"."name" ASC')
  })

  it('order by column desc', () => {
    const parts = base({ orderBy: [{ column: col('t0', 'age'), direction: 'desc' }] })
    const { sql } = dialect.generate(parts, [])
    expect(sql).toContain('ORDER BY "t0"."age" DESC')
  })

  it('multiple order by', () => {
    const parts = base({
      orderBy: [
        { column: col('t0', 'name'), direction: 'asc' },
        { column: col('t0', 'age'), direction: 'desc' },
      ],
    })
    const { sql } = dialect.generate(parts, [])
    expect(sql).toContain('ORDER BY "t0"."name" ASC, "t0"."age" DESC')
  })
})

describe('Trino — LIMIT/OFFSET additional', () => {
  it('limit', () => {
    const { sql } = dialect.generate(base({ limit: 10 }), [])
    expect(sql).toContain('LIMIT 10')
  })

  it('offset', () => {
    const { sql } = dialect.generate(base({ offset: 20 }), [])
    expect(sql).toContain('OFFSET 20')
  })
})

describe('Trino — full query', () => {
  it('complex query combines all clauses', () => {
    const parts: SqlParts = {
      select: [col('t0', 'status')],
      distinct: true,
      from: tbl('public.orders', 't0'),
      joins: [
        {
          type: 'inner',
          table: tbl('public.users', 't1'),
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
    const { sql, params } = dialect.generate(parts, [18, 2])

    expect(sql).toBe(
      'SELECT DISTINCT "t0"."status" AS "t0__status", COUNT(*) AS "cnt"' +
        ' FROM "public"."orders" AS "t0"' +
        ' INNER JOIN "public"."users" AS "t1" ON "t0"."user_id" = "t1"."id"' +
        ' WHERE "t1"."age" >= ?' +
        ' GROUP BY "t0"."status"' +
        ' HAVING "cnt" > ?' +
        ' ORDER BY "cnt" DESC' +
        ' LIMIT 5' +
        ' OFFSET 0',
    )
    expect(params).toEqual([18, 2])
  })

  it('param order preserved with multiple where clauses', () => {
    const where: WhereGroup = {
      logic: 'and',
      conditions: [
        { column: col('t0', 'name'), operator: '=', paramIndex: 0 } as WhereCondition,
        { column: col('t0', 'age'), operator: '>=', paramIndex: 1 } as WhereCondition,
        { column: col('t0', 'age'), operator: '<=', paramIndex: 2 } as WhereCondition,
      ],
    }
    const { sql, params } = dialect.generate(base({ where }), ['Alice', 18, 65])
    expect(sql).toContain('WHERE ("t0"."name" = ? AND "t0"."age" >= ? AND "t0"."age" <= ?)')
    expect(params).toEqual(['Alice', 18, 65])
  })
})
