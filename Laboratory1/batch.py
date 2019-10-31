from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

cluster = Cluster()
session = cluster.connect("dblindprorok")

query = SimpleStatement("""BEGIN BATCH
	INSERT INTO Groups(code, study_books, subjects, code_number, year, semester)
	VALUES ('KD-61', {'KD6101', 'KD6102', 'KD6103'}, {'math', 'english', 'BD'}, 61, 2007, 1);
	INSERT INTO Subjects(name, groups, max_mark, curr_max_mark, marks, year, semester)
	VALUES ('math', {'KD-61', 'KT-61', 'KP-61'}, 100, 60, {'KD6101': [12, 12, 12, 12, 12]}, 2007, 1);
APPLY BATCH;""", consistency_level=ConsistencyLevel.ONE)
session.execute(query)
