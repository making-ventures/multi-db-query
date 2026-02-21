import { describe, expect, it } from 'vitest'
import { PostgresDialect } from '../../src/dialects/postgres.js'
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

// --- Helpers ---

const dialect = new PostgresDialect()

function col(table: string, name: string): ColumnRef {
  return { tableAlias: table, columnName: name }
}

function tbl(physical: string, alias: string): TableRef {
  return { physicalName: physical, alias }
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

// --- Tests ---

describe('PostgresDialect — SELECT', () => {
  it('simple select', () => {
    const { sql, params } = dialect.generate(base(), [])
    expect(sql).toBe('SELECT "t0"."id" AS "t0__id", "t0"."name" AS "t0__name" FROM "public"."users" AS "t0"')
    expect(params).toEqual([])
  })

  it('select distinct', () => {
    const { sql } = dialect.generate(base({ distinct: true }), [])
    expect(sql).toBe('SELECT DISTINCT "t0"."id" AS "t0__id", "t0"."name" AS "t0__name" FROM "public"."users" AS "t0"')
  })

  it('count mode', () => {
    const { sql } = dialect.generate(base({ select: [], countMode: true }), [])
    expect(sql).toBe('SELECT COUNT(*) FROM "public"."users" AS "t0"')
  })

  it('empty select falls back to *', () => {
    const { sql } = dialect.generate(base({ select: [] }), [])
    expect(sql).toBe('SELECT * FROM "public"."users" AS "t0"')
  })
})

describe('PostgresDialect — JOIN', () => {
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

describe('PostgresDialect — WHERE standard', () => {
  it('equals', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: '=', paramIndex: 0 }
    const { sql, params } = dialect.generate(base({ where: cond }), ['Alice'])
    expect(sql).toContain('WHERE "t0"."name" = $1')
    expect(params).toEqual(['Alice'])
  })

  it('not equals', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: '!=', paramIndex: 0 }
    const { sql } = dialect.generate(base({ where: cond }), ['Alice'])
    expect(sql).toContain('WHERE "t0"."name" != $1')
  })

  it('less than', () => {
    const cond: WhereCondition = { column: col('t0', 'age'), operator: '<', paramIndex: 0 }
    const { sql } = dialect.generate(base({ where: cond }), [30])
    expect(sql).toContain('WHERE "t0"."age" < $1')
  })

  it('greater than or equal', () => {
    const cond: WhereCondition = { column: col('t0', 'age'), operator: '>=', paramIndex: 0 }
    const { sql } = dialect.generate(base({ where: cond }), [18])
    expect(sql).toContain('WHERE "t0"."age" >= $1')
  })
})

describe('PostgresDialect — WHERE isNull/isNotNull', () => {
  it('IS NULL', () => {
    const cond: WhereCondition = { column: col('t0', 'age'), operator: 'isNull' }
    const { sql, params } = dialect.generate(base({ where: cond }), [])
    expect(sql).toContain('WHERE "t0"."age" IS NULL')
    expect(params).toEqual([])
  })

  it('IS NOT NULL', () => {
    const cond: WhereCondition = { column: col('t0', 'age'), operator: 'isNotNull' }
    const { sql } = dialect.generate(base({ where: cond }), [])
    expect(sql).toContain('WHERE "t0"."age" IS NOT NULL')
  })
})

describe('PostgresDialect — WHERE IN / NOT IN', () => {
  it('in with uuid type', () => {
    const cond: WhereCondition = { column: col('t0', 'id'), operator: 'in', paramIndex: 0, columnType: 'uuid' }
    const { sql, params } = dialect.generate(base({ where: cond }), [['id1', 'id2']])
    expect(sql).toContain('WHERE "t0"."id" = ANY($1::uuid[])')
    expect(params).toEqual([['id1', 'id2']])
  })

  it('notIn with string type', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'notIn', paramIndex: 0, columnType: 'string' }
    const { sql } = dialect.generate(base({ where: cond }), [['a', 'b']])
    expect(sql).toContain('WHERE "t0"."name" <> ALL($1::text[])')
  })

  it('in with int type', () => {
    const cond: WhereCondition = { column: col('t0', 'age'), operator: 'in', paramIndex: 0, columnType: 'int' }
    const { sql } = dialect.generate(base({ where: cond }), [[1, 2, 3]])
    expect(sql).toContain('WHERE "t0"."age" = ANY($1::integer[])')
  })

  it('in defaults to text[] when type unknown', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'in', paramIndex: 0 }
    const { sql } = dialect.generate(base({ where: cond }), [['a']])
    expect(sql).toContain('= ANY($1::text[])')
  })
})

describe('PostgresDialect — WHERE LIKE', () => {
  it('like raw pattern', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'like', paramIndex: 0 }
    const { sql, params } = dialect.generate(base({ where: cond }), ['%Alice%'])
    expect(sql).toContain('WHERE "t0"."name" LIKE $1')
    expect(params).toEqual(['%Alice%'])
  })

  it('notLike', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'notLike', paramIndex: 0 }
    const { sql } = dialect.generate(base({ where: cond }), ['%x%'])
    expect(sql).toContain('WHERE "t0"."name" NOT LIKE $1')
  })

  it('ilike', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'ilike', paramIndex: 0 }
    const { sql } = dialect.generate(base({ where: cond }), ['%Alice%'])
    expect(sql).toContain('WHERE "t0"."name" ILIKE $1')
  })

  it('notIlike', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'notIlike', paramIndex: 0 }
    const { sql } = dialect.generate(base({ where: cond }), ['x'])
    expect(sql).toContain('WHERE "t0"."name" NOT ILIKE $1')
  })
})

describe('PostgresDialect — WHERE pattern wrapping', () => {
  it('startsWith', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'startsWith', paramIndex: 0 }
    const { sql, params } = dialect.generate(base({ where: cond }), ['Ali'])
    expect(sql).toContain('WHERE "t0"."name" LIKE $1')
    expect(params).toEqual(['Ali%'])
  })

  it('endsWith', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'endsWith', paramIndex: 0 }
    const { sql, params } = dialect.generate(base({ where: cond }), ['ice'])
    expect(sql).toContain('WHERE "t0"."name" LIKE $1')
    expect(params).toEqual(['%ice'])
  })

  it('istartsWith', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'istartsWith', paramIndex: 0 }
    const { sql, params } = dialect.generate(base({ where: cond }), ['ali'])
    expect(sql).toContain('WHERE "t0"."name" ILIKE $1')
    expect(params).toEqual(['ali%'])
  })

  it('iendsWith', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'iendsWith', paramIndex: 0 }
    const { sql, params } = dialect.generate(base({ where: cond }), ['ICE'])
    expect(sql).toContain('WHERE "t0"."name" ILIKE $1')
    expect(params).toEqual(['%ICE'])
  })

  it('contains', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'contains', paramIndex: 0 }
    const { sql, params } = dialect.generate(base({ where: cond }), ['lic'])
    expect(sql).toContain('WHERE "t0"."name" LIKE $1')
    expect(params).toEqual(['%lic%'])
  })

  it('icontains', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'icontains', paramIndex: 0 }
    const { sql, params } = dialect.generate(base({ where: cond }), ['LIC'])
    expect(sql).toContain('WHERE "t0"."name" ILIKE $1')
    expect(params).toEqual(['%LIC%'])
  })

  it('notContains', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'notContains', paramIndex: 0 }
    const { sql, params } = dialect.generate(base({ where: cond }), ['bad'])
    expect(sql).toContain('WHERE "t0"."name" NOT LIKE $1')
    expect(params).toEqual(['%bad%'])
  })

  it('notIcontains', () => {
    const cond: WhereCondition = { column: col('t0', 'name'), operator: 'notIcontains', paramIndex: 0 }
    const { sql, params } = dialect.generate(base({ where: cond }), ['BAD'])
    expect(sql).toContain('WHERE "t0"."name" NOT ILIKE $1')
    expect(params).toEqual(['%BAD%'])
  })
})

describe('PostgresDialect — wildcard escaping', () => {
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

describe('PostgresDialect — WHERE BETWEEN', () => {
  it('between', () => {
    const cond: WhereBetween = { column: col('t0', 'age'), fromParamIndex: 0, toParamIndex: 1 }
    const { sql, params } = dialect.generate(base({ where: cond }), [18, 65])
    expect(sql).toContain('WHERE "t0"."age" BETWEEN $1 AND $2')
    expect(params).toEqual([18, 65])
  })

  it('not between', () => {
    const cond: WhereBetween = { column: col('t0', 'age'), not: true, fromParamIndex: 0, toParamIndex: 1 }
    const { sql } = dialect.generate(base({ where: cond }), [0, 17])
    expect(sql).toContain('WHERE "t0"."age" NOT BETWEEN $1 AND $2')
  })
})

describe('PostgresDialect — WHERE levenshtein', () => {
  it('levenshteinLte', () => {
    const cond: WhereFunction = {
      fn: 'levenshtein',
      column: col('t0', 'name'),
      fnParamIndex: 0,
      operator: '<=',
      compareParamIndex: 1,
    }
    const { sql, params } = dialect.generate(base({ where: cond }), ['test', 2])
    expect(sql).toContain('WHERE levenshtein("t0"."name", $1) <= $2')
    expect(params).toEqual(['test', 2])
  })
})

describe('PostgresDialect — WHERE array operators', () => {
  it('arrayContains → = ANY(col)', () => {
    const cond: WhereArrayCondition = {
      column: col('t0', 'tags'),
      operator: 'contains',
      paramIndexes: [0],
      elementType: 'string',
    }
    const { sql, params } = dialect.generate(base({ where: cond }), ['urgent'])
    expect(sql).toContain('WHERE $1::text = ANY("t0"."tags")')
    expect(params).toEqual(['urgent'])
  })

  it('arrayContainsAll → @>', () => {
    const cond: WhereArrayCondition = {
      column: col('t0', 'tags'),
      operator: 'containsAll',
      paramIndexes: [0],
      elementType: 'string',
    }
    const { sql, params } = dialect.generate(base({ where: cond }), [['a', 'b']])
    expect(sql).toContain('WHERE "t0"."tags" @> $1::text[]')
    expect(params).toEqual([['a', 'b']])
  })

  it('arrayContainsAny → &&', () => {
    const cond: WhereArrayCondition = {
      column: col('t0', 'tags'),
      operator: 'containsAny',
      paramIndexes: [0],
      elementType: 'string',
    }
    const { sql } = dialect.generate(base({ where: cond }), [['x', 'y']])
    expect(sql).toContain('WHERE "t0"."tags" && $1::text[]')
  })

  it('arrayIsEmpty → cardinality = 0', () => {
    const cond: WhereArrayCondition = {
      column: col('t0', 'tags'),
      operator: 'isEmpty',
      elementType: 'string',
    }
    const { sql, params } = dialect.generate(base({ where: cond }), [])
    expect(sql).toContain('WHERE cardinality("t0"."tags") = 0')
    expect(params).toEqual([])
  })

  it('arrayIsNotEmpty → cardinality > 0', () => {
    const cond: WhereArrayCondition = {
      column: col('t0', 'tags'),
      operator: 'isNotEmpty',
      elementType: 'string',
    }
    const { sql } = dialect.generate(base({ where: cond }), [])
    expect(sql).toContain('WHERE cardinality("t0"."tags") > 0')
  })
})

describe('PostgresDialect — WHERE column-to-column', () => {
  it('column comparison', () => {
    const cond: WhereColumnCondition = {
      leftColumn: col('t0', 'total'),
      operator: '>',
      rightColumn: col('t1', 'limit'),
    }
    const { sql } = dialect.generate(base({ where: cond }), [])
    expect(sql).toContain('WHERE "t0"."total" > "t1"."limit"')
  })
})

describe('PostgresDialect — WHERE groups', () => {
  it('OR group', () => {
    const cond: WhereGroup = {
      logic: 'or',
      conditions: [
        { column: col('t0', 'name'), operator: '=', paramIndex: 0 } as WhereCondition,
        { column: col('t0', 'name'), operator: '=', paramIndex: 1 } as WhereCondition,
      ],
    }
    const { sql, params } = dialect.generate(base({ where: cond }), ['Alice', 'Bob'])
    expect(sql).toContain('WHERE ("t0"."name" = $1 OR "t0"."name" = $2)')
    expect(params).toEqual(['Alice', 'Bob'])
  })

  it('AND group', () => {
    const cond: WhereGroup = {
      logic: 'and',
      conditions: [
        { column: col('t0', 'age'), operator: '>=', paramIndex: 0 } as WhereCondition,
        { column: col('t0', 'age'), operator: '<=', paramIndex: 1 } as WhereCondition,
      ],
    }
    const { sql } = dialect.generate(base({ where: cond }), [18, 65])
    expect(sql).toContain('WHERE ("t0"."age" >= $1 AND "t0"."age" <= $2)')
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
    expect(sql).toContain('WHERE NOT ("t0"."name" = $1 OR "t0"."name" = $2)')
  })

  it('single-element group — no parens', () => {
    const cond: WhereGroup = {
      logic: 'and',
      conditions: [{ column: col('t0', 'age'), operator: '>', paramIndex: 0 } as WhereCondition],
    }
    const { sql } = dialect.generate(base({ where: cond }), [10])
    expect(sql).toContain('WHERE "t0"."age" > $1')
    // No wrapping parens for single condition
    expect(sql).not.toContain('("t0"."age"')
  })
})

describe('PostgresDialect — WHERE EXISTS', () => {
  it('EXISTS', () => {
    const cond: WhereExists = {
      exists: true,
      subquery: {
        from: tbl('public.orders', 's0'),
        join: { leftColumn: col('t0', 'id'), rightColumn: col('s0', 'user_id') },
      },
    }
    const { sql } = dialect.generate(base({ where: cond }), [])
    expect(sql).toContain('WHERE EXISTS (SELECT 1 FROM "public"."orders" AS "s0" WHERE "t0"."id" = "s0"."user_id")')
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
    expect(sql).toContain('WHERE NOT EXISTS (SELECT 1')
  })

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
    expect(sql).toContain('WHERE "t0"."id" = "s0"."user_id" AND "s0"."status" = $1')
    expect(params).toEqual(['active'])
  })
})

describe('PostgresDialect — WHERE counted subquery', () => {
  it('counted subquery >=', () => {
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
      'WHERE (SELECT COUNT(*) FROM (SELECT 1 FROM "public"."orders" AS "s0" WHERE "t0"."id" = "s0"."user_id" LIMIT 5) AS "_c") >= $1',
    )
    expect(params).toEqual([5])
  })
})

describe('PostgresDialect — GROUP BY + aggregations', () => {
  it('group by with count', () => {
    const parts = base({
      select: [col('t0', 'status')],
      from: tbl('public.orders', 't0'),
      groupBy: [col('t0', 'status')],
      aggregations: [{ fn: 'count', column: '*', alias: 'cnt' }],
    })
    const { sql } = dialect.generate(parts, [])
    expect(sql).toContain('SELECT "t0"."status" AS "t0__status", COUNT(*) AS "cnt"')
    expect(sql).toContain('GROUP BY "t0"."status"')
  })

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

describe('PostgresDialect — HAVING', () => {
  it('simple having', () => {
    const parts = base({
      select: [col('t0', 'status')],
      from: tbl('public.orders', 't0'),
      groupBy: [col('t0', 'status')],
      aggregations: [{ fn: 'count', column: '*', alias: 'cnt' }],
      having: { column: 'cnt', operator: '>', paramIndex: 0 },
    })
    const { sql, params } = dialect.generate(parts, [5])
    expect(sql).toContain('HAVING "cnt" > $1')
    expect(params).toEqual([5])
  })

  it('having between', () => {
    const parts = base({
      select: [col('t0', 'status')],
      from: tbl('public.orders', 't0'),
      groupBy: [col('t0', 'status')],
      aggregations: [{ fn: 'sum', column: col('t0', 'total'), alias: 'total' }],
      having: { alias: 'total', fromParamIndex: 0, toParamIndex: 1 },
    })
    const { sql, params } = dialect.generate(parts, [100, 1000])
    expect(sql).toContain('HAVING "total" BETWEEN $1 AND $2')
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
    expect(sql).toContain('HAVING "total" NOT BETWEEN $1 AND $2')
    expect(params).toEqual([100, 1000])
  })
})

describe('PostgresDialect — ORDER BY', () => {
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

  it('order by aggregation alias', () => {
    const parts = base({
      select: [col('t0', 'status')],
      from: tbl('public.orders', 't0'),
      groupBy: [col('t0', 'status')],
      aggregations: [{ fn: 'count', column: '*', alias: 'cnt' }],
      orderBy: [{ column: 'cnt', direction: 'desc' }],
    })
    const { sql } = dialect.generate(parts, [])
    expect(sql).toContain('ORDER BY "cnt" DESC')
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

describe('PostgresDialect — LIMIT / OFFSET', () => {
  it('limit', () => {
    const { sql } = dialect.generate(base({ limit: 10 }), [])
    expect(sql).toContain('LIMIT 10')
  })

  it('offset', () => {
    const { sql } = dialect.generate(base({ offset: 20 }), [])
    expect(sql).toContain('OFFSET 20')
  })

  it('limit + offset', () => {
    const { sql } = dialect.generate(base({ limit: 10, offset: 20 }), [])
    expect(sql).toContain('LIMIT 10 OFFSET 20')
  })
})

describe('PostgresDialect — full query', () => {
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
        ' WHERE "t1"."age" >= $1' +
        ' GROUP BY "t0"."status"' +
        ' HAVING "cnt" > $2' +
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
    expect(sql).toContain('WHERE ("t0"."name" = $1 AND "t0"."age" >= $2 AND "t0"."age" <= $3)')
    expect(params).toEqual(['Alice', 18, 65])
  })
})

describe('PostgresDialect — type casts', () => {
  it('decimal → numeric[]', () => {
    const cond: WhereCondition = { column: col('t0', 'price'), operator: 'in', paramIndex: 0, columnType: 'decimal' }
    const { sql } = dialect.generate(base({ where: cond }), [[1.5]])
    expect(sql).toContain('$1::numeric[]')
  })

  it('boolean → bool[]', () => {
    const cond: WhereCondition = { column: col('t0', 'active'), operator: 'in', paramIndex: 0, columnType: 'boolean' }
    const { sql } = dialect.generate(base({ where: cond }), [[true]])
    expect(sql).toContain('$1::bool[]')
  })

  it('date → date[]', () => {
    const cond: WhereCondition = { column: col('t0', 'created'), operator: 'in', paramIndex: 0, columnType: 'date' }
    const { sql } = dialect.generate(base({ where: cond }), [['2024-01-01']])
    expect(sql).toContain('$1::date[]')
  })

  it('timestamp → timestamp[]', () => {
    const cond: WhereCondition = { column: col('t0', 'ts'), operator: 'in', paramIndex: 0, columnType: 'timestamp' }
    const { sql } = dialect.generate(base({ where: cond }), [['2024-01-01T00:00:00Z']])
    expect(sql).toContain('$1::timestamp[]')
  })
})
