from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

cluster = Cluster()
session = cluster.connect("dblindprorok")  # or dblindprorok

query = SimpleStatement("""INSERT INTO Groups (code, study_books, subjects, code_number, year, semester)
VALUES (%s, {%s, %s, %s}, {%s, %s, %s}, %s, %s, %s);""", consistency_level=ConsistencyLevel.ONE)
session.execute(query, ('KD-61', 'KD6101', 'KD6102', 'KD6103', 'math', 'english', 'BD', 61, 2006, 1))
query.consistency_level = ConsistencyLevel.ONE
session.execute(query, ('KT-61', 'KT6101', 'KT6102', 'KT6103', 'math', 'english', 'BD', 61, 2006, 1))
query.consistency_level = ConsistencyLevel.ONE
session.execute(query, ('KP-61', 'KP6101', 'KP6102', 'KP6103', 'math', 'english', 'BD', 61, 2006, 1))
query.consistency_level = ConsistencyLevel.ONE
rows = session.execute("SELECT * FROM Groups;")
for row in rows:
    print(row)

query = SimpleStatement("""INSERT INTO Subjects (name, groups, max_mark, curr_max_mark, marks, year, semester)
VALUES (%s, {%s, %s, %s}, %s, %s, {%s: [%s, %s, %s, %s, %s]}, %s, %s);""", consistency_level=ConsistencyLevel.ONE)
session.execute(query, ('math', 'KD-61', 'KT-61', 'KP-61', 100, 60, 'KD6101', 12, 12, 12, 12, 12, 2006, 1))
query.consistency_level = ConsistencyLevel.ONE
session.execute(query, ('english', 'KD-61', 'KT-61', 'KP-61', 100, 60, 'KD6101', 12, 12, 12, 10, 12, 2006, 1))
query.consistency_level = ConsistencyLevel.ONE
session.execute(query, ('ukr', 'KD-61', 'KT-61', 'KP-61', 100, 60, 'KD6102', 12, 12, 12, 0, 12, 2006, 1))
query.consistency_level = ConsistencyLevel.ONE
rows = session.execute("SELECT * FROM Subjects;")
for row in rows:
    print(row)

query = SimpleStatement("""INSERT INTO Students (name, group, study_book, status, destiny, subjects, marks, year, semester)
VALUES ({first_name: %s, last_name: %s}, %s, %s, %s, %s, {%s, %s, %s}, {%s: [%s, %s, %s, %s, %s]}, %s, %s);""",
                        consistency_level=ConsistencyLevel.ONE)
session.execute(query, ('Lucking', 'Burger', 'KD-61', 'KD6101', 'good', 'passed', 'math', 'english', 'ukr', 'math',
                        12, 12, 12, 12, 12, 2006, 1))
query.consistency_level = ConsistencyLevel.ONE
session.execute(query, ('Lucking', 'Burger', 'KT-61', 'KT6101', 'good', 'passed', 'math', 'english', 'ukr', 'english',
                        12, 12, 12, 10, 12, 2006, 1))
query.consistency_level = ConsistencyLevel.ONE
session.execute(query, ('Mr', 'Grace', 'KD-62', 'KD6201', 'good', 'passed', 'math', 'english', 'ukr', 'ukr',
                        12, 12, 12, 0, 12, 2006, 1))
query.consistency_level = ConsistencyLevel.ONE
rows = session.execute("SELECT * FROM Students;")
for row in rows:
    print(row)

query = SimpleStatement("""INSERT INTO SubjectSheets(group, subject, marks, year, semester)
VALUES (%s, %s, {%s: [%s, %s, %s, %s, %s]}, %s, %s);""",
                        consistency_level=ConsistencyLevel.ONE)
session.execute(query, ('KD-61', 'math', 'KD6101', 12, 12, 12, 12, 12, 2006, 1))
query.consistency_level = ConsistencyLevel.ONE
session.execute(query, ('KD-61', 'english', 'KD6101', 12, 12, 12, 10, 12, 2006, 1))
query.consistency_level = ConsistencyLevel.ONE
session.execute(query, ('KD-61', 'ukr', 'KD6101', 12, 12, 12, 0, 12, 2006, 1))
query.consistency_level = ConsistencyLevel.ONE
rows = session.execute("SELECT * FROM SubjectSheets;")
for row in rows:
    print(row)


query = SimpleStatement("""UPDATE Groups
SET study_books = study_books + {%s}
WHERE code = %s AND year = %s AND semester = %s;""", consistency_level=ConsistencyLevel.ONE)
session.execute(query, ('KD6104', 'KD-61', 2006, 1))
query = SimpleStatement("""UPDATE Groups
SET code_number = %s
WHERE year = %s AND semester = %s;""", consistency_level=ConsistencyLevel.ONE)
session.execute(query, (71, 2006, 1))
query = SimpleStatement("""UPDATE Groups
SET study_books = study_books + {%s}
WHERE code = %s AND year = %s AND semester = %s;""", consistency_level=ConsistencyLevel.ONE)
session.execute(query, ('KD6106', 'KD-61', 2006, 1))
query.consistency_level = ConsistencyLevel.ONE
rows = session.execute("SELECT * FROM Groups;")
for row in rows:
    print(row)

query = SimpleStatement("""UPDATE Students
SET name = {first_name: %s, last_name: %s}
WHERE study_book = %s AND group = %s AND year = %s AND semester = %s;""", consistency_level=ConsistencyLevel.ONE)
session.execute(query, ('Mr', 'Cat', 'KD6101', 'KD-61', 2006, 1))
query.consistency_level = ConsistencyLevel.ONE
session.execute(query, ('Mr', 'Lol', 'KD6101', 'KD-61', 2006, 1))
query.consistency_level = ConsistencyLevel.ONE
session.execute(query, ('Mr', 'Scraa', 'KD6101', 'KD-61', 2006, 1))
query.consistency_level = ConsistencyLevel.ONE
rows = session.execute("SELECT * FROM Students;")
for row in rows:
    print(row)

query = SimpleStatement("""UPDATE Subjects
SET curr_max_mark = %s
WHERE name = %s AND year = %s AND semester = %s;""", consistency_level=ConsistencyLevel.ONE)
session.execute(query, (62, 'math', 2006, 1))
query.consistency_level = ConsistencyLevel.ONE
session.execute(query, (63, 'math', 2006, 1))
query.consistency_level = ConsistencyLevel.ONE
session.execute(query, (65, 'math', 2006, 1))
query.consistency_level = ConsistencyLevel.ONE
rows = session.execute("SELECT * FROM Subjects;")
for row in rows:
    print(row)

query = SimpleStatement("""UPDATE SubjectSheets
SET marks = marks + {%s: [%s, %s, %s, %s, %s]}
WHERE group = %s AND subject = %s AND year = %s AND semester = %s;""", consistency_level=ConsistencyLevel.ONE)
session.execute(query, ('KT6101', 12, 11, 10, 12, 6, 'KT-61', 'math', 2006, 1))
query.consistency_level = ConsistencyLevel.ONE
session.execute(query, ('KP6101', 12, 11, 8, 12, 6, 'KP-61', 'english', 2006, 1))
query.consistency_level = ConsistencyLevel.ONE
session.execute(query, ('KT6101', 12, 11, 5, 12, 6, 'KT-61', 'ukr', 2006, 1))
query.consistency_level = ConsistencyLevel.ONE
rows = session.execute("SELECT * FROM SubjectSheets;")
for row in rows:
    print(row)


rows = session.execute("""SELECT name, max_mark
FROM Subjects;""")
for row in rows:
    print(row)
rows = session.execute("""SELECT group, subject, marks
FROM SubjectSheets;""")
for row in rows:
    print(row)
rows = session.execute("""SELECT study_book, status
FROM Students;""")
for row in rows:
    print(row)
rows = session.execute("""SELECT study_book, marks
FROM Students;""")
for row in rows:
    print(row)


query = SimpleStatement("""DELETE subjects FROM Groups
WHERE year = %s AND semester = %s AND code = %s;""", consistency_level=ConsistencyLevel.ONE)
session.execute(query, (2006, 1, 'KD-61'))
query.consistency_level = ConsistencyLevel.ONE
session.execute(query, (2006, 1, 'KT-61'))
query.consistency_level = ConsistencyLevel.ONE
session.execute(query, (2006, 1, 'KD-62'))
query.consistency_level = ConsistencyLevel.ONE
rows = session.execute("SELECT * FROM Groups;")
for row in rows:
    print(row)

query = SimpleStatement("""DELETE curr_max_mark, max_mark, marks FROM Subjects
WHERE year = %s AND semester = %s AND name = %s;""", consistency_level=ConsistencyLevel.ONE)
session.execute(query, (2006, 1, 'math'))
query.consistency_level = ConsistencyLevel.ONE
session.execute(query, (2006, 1, 'english'))
query.consistency_level = ConsistencyLevel.ONE
session.execute(query, (2006, 1, 'ukr'))
query.consistency_level = ConsistencyLevel.ONE
rows = session.execute("SELECT * FROM Subjects;")
for row in rows:
    print(row)

query = SimpleStatement("""DELETE marks FROM Students
WHERE year = %s AND semester = %s AND study_book = %s AND group = %s;""",
                        consistency_level=ConsistencyLevel.ONE)
session.execute(query, (2006, 1, 'KD6101', 'KD-61'))
query.consistency_level = ConsistencyLevel.ONE
session.execute(query, (2006, 1, 'KT6101', 'KT-61'))
query.consistency_level = ConsistencyLevel.ONE
session.execute(query, (2006, 1, 'KD6201', 'KD-62'))
query.consistency_level = ConsistencyLevel.ONE
rows = session.execute("SELECT * FROM Students;")
for row in rows:
    print(row)

query = SimpleStatement("""DELETE marks FROM SubjectSheets
WHERE year = %s AND semester = %s AND group = %s AND subject = %s;""",
                        consistency_level=ConsistencyLevel.ONE)
session.execute(query, (2006, 1, 'KD-61', 'math'))
query.consistency_level = ConsistencyLevel.ONE
session.execute(query, (2006, 1, 'KD-61', 'english'))
query.consistency_level = ConsistencyLevel.ONE
session.execute(query, (2006, 1, 'KD-61', 'ukr'))
query.consistency_level = ConsistencyLevel.ONE
rows = session.execute("SELECT * FROM SubjectSheets;")
for row in rows:
    print(row)