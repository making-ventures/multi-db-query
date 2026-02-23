import type { DialectTestConfig } from './sharedDialectTests.js'

export const chConfig: DialectTestConfig = {
  name: 'ClickHouseDialect',
  schema: 'default',
  subSchema: 'default',

  // ── SELECT
  simpleSelect: { sql: ['SELECT `t0`.`id` AS `t0__id`, `t0`.`name` AS `t0__name` FROM `default`.`users` AS `t0`'] },
  distinct: { sql: ['SELECT DISTINCT'] },
  countMode: { sql: ['SELECT COUNT(*) FROM `default`.`users` AS `t0`'] },
  emptySelect: { sql: ['SELECT * FROM `default`.`users` AS `t0`'] },

  // ── JOIN
  innerJoin: { sql: ['INNER JOIN `default`.`orders` AS `t1` ON `t0`.`id` = `t1`.`user_id`'] },
  leftJoin: { sql: ['LEFT JOIN `default`.`orders` AS `t1` ON `t0`.`id` = `t1`.`user_id`'] },

  // ── WHERE standard
  equals: { sql: ['`t0`.`name` = {p1:String}'], params: ['Alice'] },
  notEquals: { sql: ['`t0`.`name` != {p1:String}'] },
  lessThan: { sql: ['`t0`.`age` < {p1:Int32}'] },
  greaterThanOrEqual: { sql: ['`t0`.`age` >= {p1:Int32}'] },

  // ── NULL
  isNull: { sql: ['`t0`.`age` IS NULL'] },
  isNotNull: { sql: ['`t0`.`age` IS NOT NULL'] },

  // ── LIKE
  like: { sql: ['`t0`.`name` LIKE {p1:String}'] },
  notLike: { sql: ['`t0`.`name` NOT LIKE {p1:String}'] },

  // ── Pattern wrapping
  startsWith: { sql: ['startsWith(`t0`.`name`, {p1:String})'], params: ['Ali'] },
  endsWith: { sql: ['endsWith(`t0`.`name`, {p1:String})'], params: ['ice'] },
  contains: { sql: ['`t0`.`name` LIKE {p1:String}'], params: ['%lic%'] },
  notContains: { sql: ['`t0`.`name` NOT LIKE {p1:String}'] },

  // ── Case-insensitive
  ilike: { sql: ['ilike(`t0`.`name`, {p1:String})'] },
  notIlike: { sql: ['NOT ilike(`t0`.`name`, {p1:String})'] },
  istartsWith: { sql: ['ilike(`t0`.`name`, {p1:String})'], params: ['ali%'] },
  iendsWith: { sql: ['ilike(`t0`.`name`, {p1:String})'], params: ['%ICE'] },
  icontains: { sql: ['ilike(`t0`.`name`, {p1:String})'], params: ['%LIC%'] },
  notIcontains: { sql: ['NOT ilike(`t0`.`name`, {p1:String})'], params: ['%BAD%'] },

  // ── Wildcard escaping
  escapesPercent: { sql: [], params: ['%100\\%%'] },
  escapesUnderscore: { sql: [], params: ['%a\\_b%'] },
  escapesBackslash: { sql: [], params: ['%a\\\\b%'] },

  // ── BETWEEN
  between: { sql: ['`t0`.`age` BETWEEN {p1:Int32} AND {p2:Int32}'] },
  notBetween: { sql: ['NOT (`t0`.`age` BETWEEN {p1:Int32} AND {p2:Int32})'] },

  // ── Levenshtein
  levenshtein: { sql: ['editDistance(`t0`.`name`, {p1:String}) <= {p2:UInt32}'] },

  // ── Array operators
  arrayContains: { sql: ['has(`t0`.`tags`, {p1:String})'] },
  arrayContainsAll: { sql: ['hasAll(`t0`.`tags`, [{p1:String}, {p2:String}])'], params: ['a', 'b'] },
  arrayContainsAny: { sql: ['hasAny(`t0`.`tags`, [{p1:String}, {p2:String}])'], params: ['x', 'y'] },
  arrayIsEmpty: { sql: ['empty(`t0`.`tags`)'] },
  arrayIsNotEmpty: { sql: ['notEmpty(`t0`.`tags`)'] },

  // ── Column comparison
  columnComparison: { sql: ['`t0`.`a` > `t1`.`b`'] },

  // ── Groups
  orGroup: { sql: ['(`t0`.`name` = {p1:String} OR `t0`.`name` = {p2:String})'] },
  andGroup: { sql: ['(`t0`.`age` >= {p1:Int32} AND `t0`.`age` <= {p2:Int32})'] },
  notGroup: { sql: ['NOT (`t0`.`name` = {p1:String} OR `t0`.`name` = {p2:String})'] },
  singleElementGroup: { sql: ['`t0`.`age` > {p1:Int32}'], notSql: ['(`t0`.`age`'] },

  // ── EXISTS
  exists: { sql: ['EXISTS (SELECT 1 FROM `default`.`orders` AS `s0` WHERE `t0`.`id` = `s0`.`user_id`)'] },
  notExists: { sql: ['NOT EXISTS (SELECT 1'] },
  existsWithSubFilters: { sql: ['`t0`.`id` = `s0`.`user_id` AND `s0`.`status` = {p1:String}'], params: ['active'] },

  // ── Counted subquery
  countedGte: {
    sql: [
      '`t0`.`id` IN (SELECT `s0`.`user_id` FROM `default`.`orders` AS `s0` GROUP BY `s0`.`user_id` HAVING COUNT(*) >= {p1:UInt64})',
    ],
  },
  countedGt: {
    sql: [
      '`t0`.`id` IN (SELECT `s0`.`user_id` FROM `default`.`orders` AS `s0` GROUP BY `s0`.`user_id` HAVING COUNT(*) > {p1:UInt64})',
    ],
  },
  countedLt: {
    sql: [
      '`t0`.`id` NOT IN (SELECT `s0`.`user_id` FROM `default`.`orders` AS `s0` GROUP BY `s0`.`user_id` HAVING COUNT(*) >= {p1:UInt64})',
    ],
  },
  countedLte: {
    sql: [
      '`t0`.`id` NOT IN (SELECT `s0`.`user_id` FROM `default`.`orders` AS `s0` GROUP BY `s0`.`user_id` HAVING COUNT(*) > {p1:UInt64})',
    ],
  },

  // ── GROUP BY + aggregations
  groupByCount: { sql: ['COUNT(*) AS `cnt`', 'GROUP BY `t0`.`status`'] },
  sumAgg: { sql: ['SUM(`t0`.`total`) AS `total`'] },
  avgMinMax: {
    sql: [
      'AVG(`t0`.`total`) AS `avg_amount`',
      'MIN(`t0`.`total`) AS `min_amount`',
      'MAX(`t0`.`total`) AS `max_amount`',
    ],
  },

  // ── HAVING
  having: { sql: ['HAVING `cnt` > {p1:Int32}'], params: [5] },
  havingBetween: { sql: ['HAVING `total` BETWEEN {p1:Int32} AND {p2:Int32}'], params: [100, 1000] },
  havingNotBetween: { sql: ['HAVING NOT (`total` BETWEEN {p1:Int32} AND {p2:Int32})'], params: [100, 1000] },

  // ── ORDER BY
  orderAsc: { sql: ['ORDER BY `t0`.`name` ASC'] },
  orderDesc: { sql: ['ORDER BY `t0`.`age` DESC'] },
  orderAggAlias: { sql: ['ORDER BY `cnt` DESC'] },
  multipleOrder: { sql: ['ORDER BY `t0`.`name` ASC, `t0`.`age` DESC'] },

  // ── LIMIT / OFFSET
  limit: { sql: ['LIMIT 10'] },
  offset: { sql: ['OFFSET 20'] },
  limitOffset: { sql: ['LIMIT 10 OFFSET 20'] },

  // ── IN / NOT IN
  inUuid: { sql: ['`t0`.`id` IN tuple({p1:UUID}, {p2:UUID})'], params: ['id1', 'id2'] },
  notInString: { sql: ['`t0`.`name` NOT IN tuple({p1:String}, {p2:String})'], params: ['a', 'b'] },
  inInt: { sql: ['IN tuple({p1:Int32}, {p2:Int32})'], params: [1, 2] },
  inDefaultType: { sql: ['IN tuple({p1:String})'], params: ['a'] },
  inSingleElement: { sql: ['IN tuple({p1:UUID})'], params: ['only'] },

  // ── Type casts
  typeCastDecimal: { sql: ['IN tuple({p1:Decimal})'], params: [1.5] },

  // ── Float param
  floatParam: { sql: ['{p1:Float64}'] },

  // ── Catalog-qualified (CH ignores catalog)
  catalogTable: { sql: ['FROM `default`.`users` AS `t0`'] },
  catalogJoin: { sql: ['INNER JOIN `default`.`orders` AS `t1` ON `t0`.`id` = `t1`.`user_id`'] },

  // ── HAVING (extended)
  havingAndGroup: {
    sql: ['HAVING (`totalSum` > {p1:Int32} AND `cnt` > {p2:Int32})'],
    params: [100, 5],
  },
  havingOrGroup: {
    sql: ['HAVING (`totalSum` > {p1:Int32} OR `avgTotal` > {p2:Int32})'],
    params: [1000, 200],
  },
  havingNotGroup: {
    sql: ['HAVING NOT (`totalSum` > {p1:Int32} OR `cnt` > {p2:Int32})'],
    params: [100, 5],
  },
  havingIsNull: {
    sql: ['HAVING `discountSum` IS NULL'],
    params: [],
  },

  // ── Complex WHERE
  existsInsideOrGroup: {
    sql: [
      '(`t0`.`status` = {p1:String} OR EXISTS (SELECT 1 FROM `default`.`orders` AS `s0` WHERE `t0`.`id` = `s0`.`user_id`))',
    ],
    params: ['active'],
  },
  deeplyNestedWhere: {
    sql: [
      '(`t0`.`status` = {p1:String} OR (`t0`.`age` > {p2:Int32} AND (`t0`.`name` = {p3:String} OR `t0`.`name` = {p4:String})))',
    ],
    params: ['active', 18, 'Alice', 'Bob'],
  },
  mixedFilterGroupExists: {
    sql: [
      '(`t0`.`status` = {p1:String} AND (`t0`.`age` > {p2:Int32} OR `t0`.`age` < {p3:Int32}) AND EXISTS (SELECT 1 FROM `default`.`orders` AS `s0` WHERE `t0`.`id` = `s0`.`user_id`))',
    ],
    params: ['active', 65, 18],
  },
  countedWithSubFilters: {
    sql: [
      '(SELECT COUNT(*) FROM `default`.`orders` AS `s0` WHERE `t0`.`id` = `s0`.`user_id` AND `s0`.`status` = {p1:String}) = {p2:UInt64}',
    ],
    params: ['paid', 2],
  },

  // ── Nested EXISTS
  nestedExists: {
    sql: [
      'EXISTS (SELECT 1 FROM `default`.`invoices` AS `s0` WHERE `t0`.`id` = `s0`.`order_id` AND EXISTS (SELECT 1 FROM `default`.`tenants` AS `s1` WHERE `s0`.`tenant_id` = `s1`.`id`))',
    ],
    params: [],
  },

  // ── Join-related
  filterOnJoinedColumn: {
    sql: ['`t1`.`category` = {p1:String}'],
    params: ['electronics'],
  },
  threeTableJoin: {
    sql: ['LEFT JOIN `default`.`orders` AS `t1`', 'INNER JOIN `default`.`products` AS `t2`'],
    params: [],
  },
  multiJoinPerTableFilters: {
    sql: ['(`t1`.`active` = {p1:Bool} AND `t2`.`name` = {p2:String})'],
    params: [true, 'electronics'],
  },
  aggOnJoinedColumn: {
    sql: ['SUM(`t1`.`price`) AS `totalPrice`'],
    params: [],
  },

  // ── Cross-table ORDER BY
  crossTableOrderBy: {
    sql: ['ORDER BY `t1`.`created_at` DESC'],
    params: [],
  },

  // ── Array ops on int[]
  arrayContainsInt: {
    sql: ['has(`t0`.`priorities`, {p1:Int32})'],
    params: [1],
  },
  arrayContainsAllInt: {
    sql: ['hasAll(`t0`.`priorities`, [{p1:Int32}, {p2:Int32}, {p3:Int32}])'],
    params: [1, 2, 3],
  },
  arrayInGroup: {
    sql: ['(hasAny(`t0`.`tags`, [{p1:String}]) AND `t0`.`price` > {p2:Int32})'],
    params: ['sale', 10],
  },
  arrayOnJoinedTable: {
    sql: ['hasAny(`t1`.`labels`, [{p1:String}])'],
    params: ['sale'],
  },
  arrayContainsAllSingleElement: {
    sql: ['hasAll(`t0`.`tags`, [{p1:String}])'],
    params: ['sale'],
  },

  // ── distinct + groupBy
  distinctGroupBy: {
    sql: ['SELECT DISTINCT', 'GROUP BY `t0`.`status`', 'SUM(`t0`.`total`) AS `totalSum`'],
    params: [],
  },

  // ── Full query
  fullQuery: {
    sql: [
      'SELECT DISTINCT `t0`.`status` AS `t0__status`, COUNT(*) AS `cnt`' +
        ' FROM `default`.`orders` AS `t0`' +
        ' INNER JOIN `default`.`users` AS `t1` ON `t0`.`user_id` = `t1`.`id`' +
        ' WHERE `t1`.`age` >= {p1:Int32}' +
        ' GROUP BY `t0`.`status`' +
        ' HAVING `cnt` > {p2:Int32}' +
        ' ORDER BY `cnt` DESC' +
        ' LIMIT 5' +
        ' OFFSET 0',
    ],
    params: [18, 2],
  },
  paramOrder: {
    sql: ['(`t0`.`name` = {p1:String} AND `t0`.`age` >= {p2:Int32} AND `t0`.`age` <= {p3:Int32})'],
    params: ['Alice', 18, 65],
  },
}
