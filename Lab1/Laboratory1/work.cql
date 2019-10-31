INSERT INTO Groups (code, study_books, subjects, code_number, year, semester)
VALUES ('KD-61', {'KD6101', 'KD6102', 'KD6103'}, {'math', 'english', 'BD'}, 61, 2006, 1);
INSERT INTO Groups (code, study_books, subjects, code_number, year, semester)
VALUES ('KT-61', {'KT6101', 'KT6102', 'KT6103'}, {'math', 'english', 'BD'}, 61, 2006, 1);
INSERT INTO Groups (code, study_books, subjects, code_number, year, semester)
VALUES ('KP-61', {'KP6101', 'KP6102', 'KP6103'}, {'math', 'english', 'BD'}, 61, 2006, 1);
SELECT * FROM Groups;

INSERT INTO Subjects (name, groups, max_mark, curr_max_mark, marks, year, semester)
VALUES ('math', {'KD-61', 'KT-61', 'KP-61'}, 100, 60, {'KD6101': [12, 12, 12, 12, 12]}, 2006, 1);
INSERT INTO Subjects (name, groups, max_mark, curr_max_mark, marks, year, semester)
VALUES ('english', {'KD-61', 'KT-61', 'KP-61'}, 100, 60, {'KD6101': [12, 12, 12, 10, 12]}, 2006, 1);
INSERT INTO Subjects (name, groups, max_mark, curr_max_mark, marks, year, semester)
VALUES ('ukr', {'KD-61', 'KT-61', 'KP-61'}, 100, 60, {'KD6102': [12, 12, 12, 0, 12]}, 2006, 1);
SELECT * FROM Subjects;

INSERT INTO Students (name, group, study_book, status, destiny, subjects, marks, year, semester)
VALUES ({first_name: 'Lucking', last_name: 'Burger'}, 'KD-61', 'KD6101', 'good', 'passed',
	{'math', 'english', 'ukr'}, {'math': [12, 12, 12, 12, 12]}, 2006, 1);
INSERT INTO Students (name, group, study_book, status, destiny, subjects, marks, year, semester)
VALUES ({first_name: 'Lucking', last_name: 'Burger'}, 'KT-61', 'KT6101', 'good', 'passed',
	{'math', 'english', 'ukr'}, {'english': [12, 12, 12, 10, 12]}, 2006, 1);
INSERT INTO Students (name, group, study_book, status, destiny, subjects, marks, year, semester)
VALUES ({first_name: 'Mr', last_name: 'Grace'}, 'KD-62', 'KD6201', 'good', 'passed',
	{'math', 'english', 'ukr'}, {'ukr': [12, 12, 12, 0, 12]}, 2006, 1);
SELECT * FROM Students;

INSERT INTO SubjectSheets(group, subject, marks, year, semester)
VALUES ('KD-61', 'math', {'KD6101': [12, 12, 12, 12, 12]}, 2006, 1);
INSERT INTO SubjectSheets(group, subject, marks, year, semester)
VALUES ('KD-61', 'english', {'KD6101': [12, 12, 12, 10, 12]}, 2006, 1);
INSERT INTO SubjectSheets(group, subject, marks, year, semester)
VALUES ('KD-61', 'ukr', {'KD6102': [12, 12, 12, 0, 12]}, 2006, 1);
SELECT * FROM SubjectSheets;



UPDATE Groups
SET study_books = study_books + {'KD6104'}
WHERE code = 'KD-61' AND year = 2006 AND semester = 1;
UPDATE Groups
SET code_number = 71
WHERE year = 2006 AND semester = 1;
UPDATE Groups
SET study_books = study_books + {'KD6106'}
WHERE code = 'KD-61' AND year = 2006 AND semester = 1;
SELECT * FROM Groups;

UPDATE Students
SET name = {first_name: 'Mr', last_name: 'Cat'}
WHERE study_book = 'KD6101' AND group = 'KD-61' AND year = 2006 AND semester = 1;
UPDATE Students
SET name = {first_name: 'Mr', last_name: 'Lol'}
WHERE study_book = 'KD6101' AND group = 'KD-61' AND year = 2006 AND semester = 1;
UPDATE Students
SET name = {first_name: 'Mr', last_name: 'Skraa'}
WHERE study_book = 'KD6101' AND group = 'KD-61' AND year = 2006 AND semester = 1;
SELECT * FROM Students;

UPDATE Subjects
SET curr_max_mark = 62
WHERE name = 'math' AND year = 2006 AND semester = 1;
UPDATE Subjects
SET curr_max_mark = 63
WHERE name = 'math' AND year = 2006 AND semester = 1;
UPDATE Subjects
SET curr_max_mark = 65
WHERE name = 'math' AND year = 2006 AND semester = 1;
SELECT * FROM Subjects;

UPDATE SubjectSheets
SET marks = marks + {'KT6101': [12, 11, 10, 12, 6]}
WHERE group = 'KT-61' AND subject = 'math' AND year = 2006 AND semester = 1;
UPDATE SubjectSheets
SET marks = marks + {'KP6101': [12, 11, 8, 12, 6]}
WHERE group = 'KP-61' AND subject = 'english' AND year = 2006 AND semester = 1;
UPDATE SubjectSheets
SET marks = marks + {'KT6101': [12, 11, 5, 12, 6]}
WHERE group = 'KT-61' AND subject = 'ukr' AND year = 2006 AND semester = 1;
SELECT * FROM SubjectSheets;



SELECT name, max_mark
FROM Subjects;

SELECT group, subject, marks
FROM SubjectSheets;

SELECT study_book, status
FROM Students;

SELECT study_book, marks
FROM Students;



DELETE subjects FROM Groups
WHERE year = 2006 AND semester = 1 AND code = 'KD-61';
DELETE subjects FROM Groups
WHERE year = 2006 AND semester = 1 AND code = 'KT-61';
DELETE subjects FROM Groups
WHERE year = 2006 AND semester = 1 AND code = 'KD-62';
SELECT * FROM Groups;

DELETE curr_max_mark, max_mark, marks FROM Subjects
WHERE year = 2006 AND semester = 1 AND name = 'math';
DELETE curr_max_mark, max_mark, marks FROM Subjects
WHERE year = 2006 AND semester = 1 AND name = 'english';
DELETE curr_max_mark, max_mark, marks FROM Subjects
WHERE year = 2006 AND semester = 1 AND name = 'ukr';
SELECT * FROM Subjects;

DELETE marks FROM Students
WHERE year = 2006 AND semester = 1 AND study_book = 'KD6101' AND group = 'KD-61';
DELETE marks FROM Students
WHERE year = 2006 AND semester = 1 AND study_book = 'KT6101' AND group = 'KT-61';
DELETE marks FROM Students
WHERE year = 2006 AND semester = 1 AND study_book = 'KD6201' AND group = 'KD-62';
SELECT * FROM Students;

DELETE marks FROM SubjectSheets
WHERE year = 2006 AND semester = 1 AND group = 'KD-61' AND subject = 'math';
DELETE marks FROM SubjectSheets
WHERE year = 2006 AND semester = 1 AND group = 'KD-61' AND subject = 'english';
DELETE marks FROM SubjectSheets
WHERE year = 2006 AND semester = 1 AND group = 'KD-61' AND subject = 'ukr';
SELECT * FROM SubjectSheets;