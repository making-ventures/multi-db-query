import type { DialectTestConfig } from './sharedDialectTests.js'

export const trinoConfig: DialectTestConfig = {
  name: 'TrinoDialect',
  schema: 'public',
  subSchema: 'public',

  // ── SELECT
  simpleSelect: { sql: ['SELECT "t0"."id" AS "t0__id", "t0"."name" AS "t0__name" FROM "public"."users" AS "t0"'] },
  distinct: { sql: ['SELECT DISTINCT'] },
  countMode: { sql: ['SELECT COUNT(*) FROM "public"."users" AS "t0"'] },
  emptySelect: { sql: ['SELECT * FROM "public"."users" AS "t0"'] },

  // ── JOIN
  innerJoin: { sql: ['INNER JOIN "public"."orders" AS "t1" ON "t0"."id" = "t1"."user_id"'] },
  leftJoin: { sql: ['LEFT JOIN "public"."orders" AS "t1" ON "t0"."id" = "t1"."user_id"'] },

  // ── WHERE standard
  equals: { sql: ['"t0"."name" = ?'], params: ['Alice'] },
  notEquals: { sql: ['"t0"."name" != ?'] },
  lessThan: { sql: ['"t0"."age" < ?'] },
  greaterThanOrEqual: { sql: ['"t0"."age" >= ?'] },

  // ── NULL
  isNull: { sql: ['"t0"."age" IS NULL'] },
  isNotNull: { sql: ['"t0"."age" IS NOT NULL'] },

  // ── LIKE
  like: { sql: ['"t0"."name" LIKE ?'], params: ['%foo%'] },
  notLike: { sql: ['"t0"."name" NOT LIKE ?'] },

  // ── Pattern wrapping
  startsWith: { sql: ['"t0"."name" LIKE ? ESCAPE \'\\\''], params: ['Ali%'] },
  endsWith: { sql: [], params: ['%ice'] },
  contains: { sql: [], params: ['%lic%'] },
  notContains: { sql: ['"t0"."name" NOT LIKE ? ESCAPE \'\\\''] },

  // ── Case-insensitive
  ilike: { sql: ['lower("t0"."name") LIKE lower(?)'] },
  notIlike: { sql: ['NOT (lower("t0"."name") LIKE lower(?))'] },
  istartsWith: { sql: ['lower("t0"."name") LIKE lower(?) ESCAPE \'\\\''], params: ['ali%'] },
  iendsWith: { sql: ['lower("t0"."name") LIKE lower(?) ESCAPE \'\\\''], params: ['%ICE'] },
  icontains: { sql: ['lower("t0"."name") LIKE lower(?) ESCAPE \'\\\''], params: ['%LIC%'] },
  notIcontains: { sql: ['NOT (lower("t0"."name") LIKE lower(?) ESCAPE \'\\\')'], params: ['%BAD%'] },

  // ── Wildcard escaping
  escapesPercent: { sql: [], params: ['%100\\%%'] },
  escapesUnderscore: { sql: [], params: ['%a\\_b%'] },
  escapesBackslash: { sql: [], params: ['%a\\\\b%'] },

  // ── BETWEEN
  between: { sql: ['"t0"."age" BETWEEN ? AND ?'] },
  notBetween: { sql: ['"t0"."age" NOT BETWEEN ? AND ?'] },

  // ── Levenshtein
  levenshtein: { sql: ['levenshtein_distance("t0"."name", ?) <= ?'] },

  // ── Array operators
  arrayContains: { sql: ['contains("t0"."tags", ?)'] },
  arrayContainsAll: { sql: ['cardinality(array_except(ARRAY[?, ?], "t0"."tags")) = 0'] },
  arrayContainsAny: { sql: ['arrays_overlap("t0"."tags", ARRAY[?, ?])'] },
  arrayIsEmpty: { sql: ['cardinality("t0"."tags") = 0'] },
  arrayIsNotEmpty: { sql: ['cardinality("t0"."tags") > 0'] },

  // ── Column comparison
  columnComparison: { sql: ['"t0"."a" > "t1"."b"'] },

  // ── Groups
  orGroup: { sql: ['("t0"."name" = ? OR "t0"."name" = ?)'], params: ['Alice', 'Bob'] },
  andGroup: { sql: ['("t0"."age" >= ? AND "t0"."age" <= ?)'] },
  notGroup: { sql: ['NOT ("t0"."name" = ? OR "t0"."name" = ?)'] },
  singleElementGroup: { sql: ['"t0"."age" > ?'], notSql: ['("t0"."age"'] },

  // ── EXISTS
  exists: { sql: ['EXISTS (SELECT 1 FROM "public"."orders" AS "s0" WHERE "t0"."id" = "s0"."user_id")'] },
  notExists: { sql: ['NOT EXISTS'] },
  existsWithSubFilters: { sql: ['"t0"."id" = "s0"."user_id" AND "s0"."status" = ?'], params: ['active'] },

  // ── Counted subquery
  countedGte: {
    sql: ['"t0"."id" IN (SELECT "s0"."user_id" FROM "public"."orders" AS "s0" GROUP BY "s0"."user_id" HAVING COUNT(*) >= ?)'],
    params: [5],
  },
  countedGt: {
    sql: ['"t0"."id" IN (SELECT "s0"."user_id" FROM "public"."orders" AS "s0" GROUP BY "s0"."user_id" HAVING COUNT(*) > ?)'],
    params: [1],
  },
  countedLt: {
    sql: ['"t0"."id" NOT IN (SELECT "s0"."user_id" FROM "public"."orders" AS "s0" GROUP BY "s0"."user_id" HAVING COUNT(*) >= ?)'],
    params: [2],
  },
  countedLte: {
    sql: ['"t0"."id" NOT IN (SELECT "s0"."user_id" FROM "public"."orders" AS "s0" GROUP BY "s0"."user_id" HAVING COUNT(*) > ?)'],
    params: [1],
  },

  // ── GROUP BY + aggregations
  groupByCount: { sql: ['COUNT(*) AS "cnt"', 'GROUP BY "t0"."status"'] },
  sumAgg: { sql: ['SUM("t0"."total") AS "total"'] },
  avgMinMax: { sql: ['AVG("t0"."total") AS "avg_amount"', 'MIN("t0"."total") AS "min_amount"', 'MAX("t0"."total") AS "max_amount"'] },

  // ── HAVING
  having: { sql: ['HAVING "cnt" > ?'], params: [5] },
  havingBetween: { sql: ['HAVING "total" BETWEEN ? AND ?'], params: [100, 1000] },
  havingNotBetween: { sql: ['HAVING "total" NOT BETWEEN ? AND ?'], params: [100, 1000] },

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
  inUuid: { sql: ['"t0"."id" IN (?, ?)'], params: ['id1', 'id2'] },
  notInString: { sql: ['"t0"."name" NOT IN (?, ?)'], params: ['a', 'b'] },
  inInt: { sql: ['IN (?, ?)'], params: [1, 2] },
  inDefaultType: { sql: ['IN (?)'], params: ['a'] },
  inSingleElement: { sql: ['"t0"."id" IN (?)'], params: ['only'] },

  // ── Catalog-qualified
  catalogTable: { sql: ['FROM "pg_main"."public"."users" AS "t0"'] },
  catalogJoin: { sql: ['INNER JOIN "pg_main"."public"."orders" AS "t1"'] },

  // ── Full query
  fullQuery: {
    sql: [
      'SELECT DISTINCT "t0"."status" AS "t0__status", COUNT(*) AS "cnt"' +
        ' FROM "public"."orders" AS "t0"' +
        ' INNER JOIN "public"."users" AS "t1" ON "t0"."user_id" = "t1"."id"' +
        ' WHERE "t1"."age" >= ?' +
        ' GROUP BY "t0"."status"' +
        ' HAVING "cnt" > ?' +
        ' ORDER BY "cnt" DESC' +
        ' LIMIT 5' +
        ' OFFSET 0',
    ],
    params: [18, 2],
  },
  paramOrder: {
    sql: ['("t0"."name" = ? AND "t0"."age" >= ? AND "t0"."age" <= ?)'],
    params: ['Alice', 18, 65],
  },
}
