import os
import sys
sys.path.append(os.path.realpath('../src'))

import CSVCatalog
import CSVTable
from CSVCatalog import ColumnDefinition

data_path = os.path.realpath('../data') + '/'

def cleanup():
    cat = CSVCatalog.CSVCatalog()
    cat.drop_table("pitching")
    cat.drop_table("people")
    cat.drop_table("batting")
    cat.drop_table("teams")


cleanup()
catalog = CSVCatalog.CSVCatalog()

cds = [ColumnDefinition('playerID'), ColumnDefinition('teamID'),
       ColumnDefinition('yearID', 'number'), ColumnDefinition('stint', 'number'),
       ColumnDefinition('H', 'number'), ColumnDefinition('AB', 'number'),
       ColumnDefinition('HR', 'number')]
t = catalog.create_table("batting", data_path + "Batting.csv", cds)
t.define_primary_key(['playerID', 'teamID', 'yearID', 'stint'])
t.define_index("pid_tid_idx", ['playerID', 'teamID'])

cds = [ColumnDefinition('playerID'), ColumnDefinition('teamID'),
       ColumnDefinition('yearID', 'number'), ColumnDefinition('stint', 'number'),
       ColumnDefinition('IPouts', 'number'), ColumnDefinition('ERA', 'number')]
t2 = catalog.create_table("pitching", data_path + "Pitching.csv", cds)
t2.define_primary_key(['playerID', 'teamID', 'yearID', 'stint'])

cds = [ColumnDefinition('playerID'), ColumnDefinition('nameLast', not_null=True),
       ColumnDefinition('nameFirst')]
t3 = catalog.create_table("people", data_path + "People.csv", cds)
t3.define_primary_key(['playerID'])
t3.define_index("ln_idx", ['nameLast'])

people = CSVTable.CSVTable('people')
batting = CSVTable.CSVTable('batting')
pitching = CSVTable.CSVTable('pitching')

print("Created tables people, batting, pitching")
