import type { DialectTestConfig } from './sharedDialectTests.js'

export const pgConfig: DialectTestConfig = {
  name: 'PostgresDialect',
  schema: 'public',
  subSchema: 'public',

  // ── SELECT
  simpleSelect: {
    sql: ['SELECT "t0"."id" AS "t0__id", "t0"."name" AS "t0__name" FROM "public"."users" AS "t0"'],
    params: [],
  },
  distinct: { sql: ['SELECT DISTINCT "t0"."id"'] },
  countMode: { sql: ['SELECT COUNT(*) FROM "public"."users" AS "t0"'] },
  emptySelect: { sql: ['SELECT * FROM "public"."users" AS "t0"'] },

  // ── JOIN
  innerJoin: { sql: ['INNER JOIN "public"."orders" AS "t1" ON "t0"."id" = "t1"."user_id"'] },
  leftJoin: { sql: ['LEFT JOIN "public"."orders" AS "t1" ON "t0"."id" = "t1"."user_id"'] },

  // ── WHERE standard
  equals: { sql: ['"t0"."name" = $1'], params: ['Alice'] },
  notEquals: { sql: ['"t0"."name" != $1'] },
  lessThan: { sql: ['"t0"."age" < $1'] },
  greaterThanOrEqual: { sql: ['"t0"."age" >= $1'] },

  // ── NULL
  isNull: { sql: ['"t0"."age" IS NULL'], params: [] },
  isNotNull: { sql: ['"t0"."age" IS NOT NULL'] },

  // ── LIKE
  like: { sql: ['"t0"."name" LIKE $1'], params: ['%foo%'] },
  notLike: { sql: ['"t0"."name" NOT LIKE $1'] },

  // ── Pattern wrapping
  startsWith: { sql: ['"t0"."name" LIKE $1'], params: ['Ali%'] },
  endsWith: { sql: ['"t0"."name" LIKE $1'], params: ['%ice'] },
  contains: { sql: ['"t0"."name" LIKE $1'], params: ['%lic%'] },
  notContains: { sql: ['"t0"."name" NOT LIKE $1'], params: ['%bad%'] },

  // ── Case-insensitive
  ilike: { sql: ['"t0"."name" ILIKE $1'] },
  notIlike: { sql: ['"t0"."name" NOT ILIKE $1'] },
  istartsWith: { sql: ['"t0"."name" ILIKE $1'], params: ['ali%'] },
  iendsWith: { sql: ['"t0"."name" ILIKE $1'], params: ['%ICE'] },
  icontains: { sql: ['"t0"."name" ILIKE $1'], params: ['%LIC%'] },
  notIcontains: { sql: ['"t0"."name" NOT ILIKE $1'], params: ['%BAD%'] },

  // ── Wildcard escaping
  escapesPercent: { sql: [], params: ['%100\\%%'] },
  escapesUnderscore: { sql: [], params: ['%a\\_b%'] },
  escapesBackslash: { sql: [], params: ['%a\\\\b%'] },

  // ── BETWEEN
  between: { sql: ['"t0"."age" BETWEEN $1 AND $2'], params: [18, 65] },
  notBetween: { sql: ['"t0"."age" NOT BETWEEN $1 AND $2'] },

  // ── Levenshtein
  levenshtein: { sql: ['levenshtein("t0"."name", $1) <= $2'], params: ['test', 2] },

  // ── Array operators
  arrayContains: { sql: ['$1::text = ANY("t0"."tags")'], params: ['urgent'] },
  arrayContainsAll: { sql: ['"t0"."tags" @> $1::text[]'], params: [['a', 'b']] },
  arrayContainsAny: { sql: ['"t0"."tags" && $1::text[]'] },
  arrayIsEmpty: { sql: ['cardinality("t0"."tags") = 0'], params: [] },
  arrayIsNotEmpty: { sql: ['cardinality("t0"."tags") > 0'] },

  // ── Column comparison
  columnComparison: { sql: ['"t0"."a" > "t1"."b"'] },

  // ── Groups
  orGroup: { sql: ['("t0"."name" = $1 OR "t0"."name" = $2)'], params: ['Alice', 'Bob'] },
  andGroup: { sql: ['("t0"."age" >= $1 AND "t0"."age" <= $2)'] },
  notGroup: { sql: ['NOT ("t0"."name" = $1 OR "t0"."name" = $2)'] },
  singleElementGroup: { sql: ['"t0"."age" > $1'], notSql: ['("t0"."age"'] },

  // ── EXISTS
  exists: { sql: ['EXISTS (SELECT 1 FROM "public"."orders" AS "s0" WHERE "t0"."id" = "s0"."user_id")'] },
  notExists: { sql: ['NOT EXISTS (SELECT 1'] },
  existsWithSubFilters: { sql: ['"t0"."id" = "s0"."user_id" AND "s0"."status" = $1'], params: ['active'] },

  // ── Counted subquery
  countedGte: {
    sql: [
      '(SELECT COUNT(*) FROM (SELECT 1 FROM "public"."orders" AS "s0" WHERE "t0"."id" = "s0"."user_id" LIMIT 5) AS "_c") >= $1',
    ],
    params: [5],
  },
  countedGt: {
    sql: [
      '(SELECT COUNT(*) FROM (SELECT 1 FROM "public"."orders" AS "s0" WHERE "t0"."id" = "s0"."user_id" LIMIT 2) AS "_c") > $1',
    ],
  },
  countedLt: {
    sql: ['(SELECT COUNT(*) FROM "public"."orders" AS "s0" WHERE "t0"."id" = "s0"."user_id") < $1'],
    params: [2],
  },
  countedLte: {
    sql: ['(SELECT COUNT(*) FROM "public"."orders" AS "s0" WHERE "t0"."id" = "s0"."user_id") <= $1'],
  },

  // ── GROUP BY + aggregations
  groupByCount: { sql: ['COUNT(*) AS "cnt"', 'GROUP BY "t0"."status"'] },
  sumAgg: { sql: ['SUM("t0"."total") AS "total"'] },
  avgMinMax: {
    sql: [
      'AVG("t0"."total") AS "avg_amount"',
      'MIN("t0"."total") AS "min_amount"',
      'MAX("t0"."total") AS "max_amount"',
    ],
  },

  // ── HAVING
  having: { sql: ['HAVING COUNT(*) > $1'], params: [5] },
  havingBetween: { sql: ['HAVING SUM("t0"."total") BETWEEN $1 AND $2'], params: [100, 1000] },
  havingNotBetween: { sql: ['HAVING SUM("t0"."total") NOT BETWEEN $1 AND $2'], params: [100, 1000] },

  // ── ORDER BY
  orderAsc: { sql: ['ORDER BY "t0"."name" ASC'] },
  orderDesc: { sql: ['ORDER BY "t0"."age" DESC'] },
  orderAggAlias: { sql: ['ORDER BY "cnt" DESC'] },
  multipleOrder: { sql: ['ORDER BY "t0"."name" ASC, "t0"."age" DESC'] },

  // ── LIMIT / OFFSET
  limit: { sql: ['LIMIT 10'] },
  offset: { sql: ['OFFSET 20'] },
  limitOffset: { sql: ['LIMIT 10 OFFSET 20'] },

  // ── IN / NOT IN
  inUuid: { sql: ['"t0"."id" = ANY($1::uuid[])'], params: [['id1', 'id2']] },
  notInString: { sql: ['"t0"."name" <> ALL($1::text[])'], params: [['a', 'b']] },
  inInt: { sql: ['"t0"."age" = ANY($1::integer[])'], params: [[1, 2]] },
  inDefaultType: { sql: ['= ANY($1::text[])'], params: [['a']] },
  inSingleElement: { sql: ['= ANY($1::uuid[])'], params: [['only']] },

  // ── Type casts
  typeCastDecimal: { sql: ['$1::numeric[]'], params: [[1.5]] },

  // ── Float param
  floatParam: { sql: ['"t0"."score" > $1'], params: [3.14] },

  // ── Catalog-qualified (PG ignores catalog)
  catalogTable: { sql: ['FROM "public"."users" AS "t0"'] },
  catalogJoin: { sql: ['INNER JOIN "public"."orders" AS "t1" ON "t0"."id" = "t1"."user_id"'] },

  // ── Full query
  fullQuery: {
    sql: [
      'SELECT DISTINCT "t0"."status" AS "t0__status", COUNT(*) AS "cnt"' +
        ' FROM "public"."orders" AS "t0"' +
        ' INNER JOIN "public"."users" AS "t1" ON "t0"."user_id" = "t1"."id"' +
        ' WHERE "t1"."age" >= $1' +
        ' GROUP BY "t0"."status"' +
        ' HAVING COUNT(*) > $2' +
        ' ORDER BY "cnt" DESC' +
        ' LIMIT 5' +
        ' OFFSET 0',
    ],
    params: [18, 2],
  },
  paramOrder: {
    sql: ['("t0"."name" = $1 AND "t0"."age" >= $2 AND "t0"."age" <= $3)'],
    params: ['Alice', 18, 65],
  },
}
