#!/usr/bin/env python

#
#
# [ 15-04-14 @ 13:52:32 % time python db_driver.py --rows=4772 --columns=100
# python db_driver.py  22.94s user 0.34s system 89% cpu 26.120 total
#
#

import argparse
import random
import MySQLdb

parser = argparse.ArgumentParser(description='Make a bunch of data, scramble it up, and check for equality.')
parser.add_argument('--rows',    type=int, required=False, default=5, help='Number of rows in result sets.')
parser.add_argument('--columns', type=int, required=False, default=5, help='Number of columns in result sets.')
args = parser.parse_args()

################################################################################
# Data definitions

db = MySQLdb.connect('localhost', 'root', '', 'TESTDB')
cursor = db.cursor()

table = 'x'
num_rows = args.rows
num_columns = args.columns
column_types = ['integer', 'float', 'varchar(255)']
columns = ['`c{}`'.format(n + 1) for n in xrange(num_columns)]
column_definitions = ['`c{}` {} not null'.format(n + 1, column_types[n % len(column_types)]) for n in xrange(num_columns)]
values = [
    [j + 1 for j in xrange(i, i + num_columns)]
    for i in xrange(0, num_rows * num_columns, num_columns)
]

################################################################################
# Closure definitions

# Create a temporary table and populate it with a basic data set.
def initializeDataSet():
    create_table = 'CREATE TEMPORARY TABLE `{}` ({})'
    insert_values = 'INSERT INTO `{}` ({}) VALUES {}'

    column_definition_str = ','.join(column_definitions);
    column_str = ','.join(columns)
    value_str = '({})'.format('),('.join([
        ",".join([
            "'{}'".format(v) for v in t
        ]) for t in values
    ]))

    cursor.execute(create_table.format(table, column_definition_str))
    cursor.execute(insert_values.format(table, column_str, value_str))

# Fetch the complete result set yielded by executing the provided query.
def fetch(query):
    cursor.execute(query)
    return cursor.fetchall()

# Fetch all data in the provided table.
def fetchPlain():
    q = 'SELECT {} FROM `{}`'
    column_str = ','.join(columns)
    return fetch(q.format(column_str, table))

# Fetch all data in the provided table with random row order.
def fetchRowRand():
    q = 'SELECT {} FROM `{}` ORDER BY RAND()'
    column_str = ','.join(columns)
    return fetch(q.format(column_str, table))

# Fetch all data in the provided table with random column order.
def fetchColRand():
    rand_columns = columns[:]
    random.shuffle(rand_columns)

    q = 'SELECT {} FROM `{}`'
    column_str = ','.join(rand_columns)
    return fetch(q.format(column_str, table))

# Fetch all data in the provided table with random row and column order.
def fetchFullRand():
    rand_columns = columns[:]
    random.shuffle(rand_columns)

    q = 'SELECT {} FROM `{}` ORDER BY RAND()'
    column_str = ','.join(rand_columns)
    return fetch(q.format(column_str, table))

################################################################################
# Function definitions

# Compare two result sets for equality with varying facets of strictness.
#
# @param enforce_row_order boolean: Should result set row ordering be identical
#  when assessing equality?
# @param enforce_column_order boolean: Should result set column ordering be
#  identical when assessing equality?
def resultSetsEqual(r1, r2, enforce_row_order, enforce_column_order):
    l1 = [[j for j in i] for i in r1]
    l2 = [[j for j in i] for i in r2]

    if not enforce_column_order:
        for i in l1: i.sort()
        for i in l2: i.sort()

    if not enforce_row_order:
        l1.sort()
        l2.sort()

    return l1 == l2

# Assert that two result sets are equal for varying definitions of "equality".
# Several comparisons are made between each result set, where row and column
# ordering may or may not be enforced.
#   0:        enforce row ordering,        enforce column ordering
#   1:        enforce row ordering, do not enforce column ordering
#   2: do not enforce row ordering,        enforce column ordering
#   3: do not enforce row ordering, do not enforce column ordering
#
# Result sets are compared bi-directionally, so the above-described assersions
# are made twice (first checking r1 ~= r2, then checking r2 ~= r1).
#
# @param assertions list: a 4-element boolean list indicating which comparisons
#  should yield equal results.
def assertComparisons(r1, r2, assertions):
    assert(resultSetsEqual(r1, r2, True,   True) == assertions[0])
    assert(resultSetsEqual(r1, r2, False,  True) == assertions[1])
    assert(resultSetsEqual(r1, r2, True,  False) == assertions[2])
    assert(resultSetsEqual(r1, r2, False, False) == assertions[3])

    assert(resultSetsEqual(r2, r1, True,   True) == assertions[0])
    assert(resultSetsEqual(r2, r1, False,  True) == assertions[1])
    assert(resultSetsEqual(r2, r1, True,  False) == assertions[2])
    assert(resultSetsEqual(r2, r1, False, False) == assertions[3])

################################################################################
# Script execution

initializeDataSet()

assertComparisons(   fetchPlain(),    fetchPlain(), [True,  True,  True,  True])
assertComparisons(   fetchPlain(),  fetchRowRand(), [False, True,  False, True])
assertComparisons(   fetchPlain(),  fetchColRand(), [False, False, True,  True])
assertComparisons(   fetchPlain(), fetchFullRand(), [False, False, False, True])
assertComparisons( fetchRowRand(),  fetchRowRand(), [False, True,  False, True])
assertComparisons( fetchRowRand(),  fetchColRand(), [False, False, False, True])
assertComparisons( fetchRowRand(), fetchFullRand(), [False, False, False, True])
assertComparisons( fetchColRand(),  fetchColRand(), [False, False, True,  True])
assertComparisons( fetchColRand(), fetchFullRand(), [False, False, False, True])
assertComparisons(fetchFullRand(), fetchFullRand(), [False, False, False, True])

db.close()
