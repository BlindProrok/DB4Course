CREATE KEYSPACE IF NOT EXISTS DBLindProrok
WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3};

CREATE TABLE IF NOT EXISTS Groups(
	code text,
	study_books set<text>,
	subjects set<text>,
	code_number int static,
	year int,
	semester int,
	PRIMARY KEY((year, semester), code)
);

CREATE TABLE IF NOT EXISTS Subjects(
	name text,
	groups set<text>,
	max_mark float,
	curr_max_mark float,
	marks map<text, frozen<list<float>>>,
	year int,
	semester int,
	PRIMARY KEY((year, semester), name)
);

CREATE TYPE IF NOT EXISTS full_name(
	first_name text,
	last_name text
);

CREATE TABLE IF NOT EXISTS Students(
	name frozen<full_name>,
	group text,
	study_book text,
	status text,
	destiny text,
	subjects set<text>,
	marks map<text, frozen<list<float>>>,
	year int,
	semester int,
	PRIMARY KEY((year, semester), study_book, group)
);

CREATE TABLE IF NOT EXISTS SubjectSheets(
	group text,
	subject text,
	marks map<text, frozen<list<float>>>,
	year int,
	semester int,
	PRIMARY KEY((year, semester), group, subject)
);