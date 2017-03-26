# https://codereview.stackexchange.com/questions/37465/compare-last-modification-time-with-specfied-time
import os
import sqlite3
import logging
import datetime


op = os.path
HERE = os.getcwd()
logging.basicConfig(filename=r".\errors.log", level=30)


class SQL:
	""" pseudo class (i.e. C-struct) to store all sql strings """
	save_file = \
		"INSERT INTO files (dType, file, time, human_time) VALUES (?,?,?,?);"
	save_error = \
		"INSERT INTO errors (dType, file, error) VALUES (?,?,?);"
	make_file_table = """
		CREATE TABLE files
		( uid        INTEGER PRIMARY KEY
		, dType      TEXT NOT NULL
		, file       VARCHAR NOT NULL
		, time       TIMESTAMP NOT NULL
		, human_time TEXT NOT NULL
		);
	"""
	make_error_table = """
		CREATE TABLE errors
		( uid   INTEGER PRIMARY KEY
		, dType TEXT NOT NULL
		, file  VARCHAR NOT NULL
		, error TEXT NOT NULL
		);
	"""


humanizeDate = lambda x: datetime.datetime.fromtimestamp(x)

def lastAccess(absFilePath, humanize=False):
	x = op.getatime(absFilePath)
	return humanizeDate(x) if humanize else x

def lastModify(absFilePath, humanize=False):
	x = op.getmtime(absFilePath)
	return humanizeDate(x) if humanize else x

def createDate(absFilePath, humanize=False):
	x = op.getctime(absFilePath)
	return humanizeDate(x) if humanize else x


def process(aFile, dateFunc, dbCursor):
	dType = dateFunc.__name__
	try:
		details = dateFunc(aFile, humanize=True)
	except OSError as errMsg:
		dbCursor.execute(
			SQL.save_error, (dType, aFile, str(errMsg),)
		)
		logging.exception("Suppressed error at: {}".format(errMsg))
	else:
		if details.year == 2017 and details.month == 1:
		# if details.year == 2017:                                              # TODO: production
			human_time = str(details)
			timestamp = dateFunc(aFile)
			dbCursor.execute(
				SQL.save_file, (dType, aFile, timestamp, human_time,)
			)
	return True

def run_check(dbCursor, dateFunc, aLocn, begin_at_root):
	os.chdir(aLocn)
	if begin_at_root:
		os.chdir(os.sep)
	for root, dirs, files in os.walk(op.abspath(".")):
		print("Now scanning: {}".format(root))
		if root == HERE:
			continue
		else:
			root = op.abspath(root)
			files = [op.join(root, _) for _ in files]
		for aFile in files:
			process(aFile, dateFunc, dbCursor)
	return


def main():
	with open("places-to-scan.txt", mode="rt") as fh:
		locations = fh.readlines()
	_stripper = lambda x: x.strip()
	_rejectEmpty = lambda x: True if x else False
	locations = filter(_rejectEmpty, map(_stripper, locations))
	locations = [_ for _ in locations]

	with sqlite3.connect("analyses.db") as conn:
		cur = conn.cursor()
		cur.execute(SQL.make_file_table)
		cur.execute(SQL.make_error_table)

		for aLocn in locations:                                                       # NOTE: testing only
			print("--------- Begin Scanning: {} ---------".format(aLocn))
			x = input("Would you like to switch to root directory?\n[y]es / [N]o >>> ").lower()
			begin_at_root = True if x.startswith("y") else False

			run_check(cur, lastAccess, aLocn, begin_at_root)
			ch = input("Access-Date Scan Complete. Press 'y' to continue to Modify-Date Scan: ")
			if not ch.lower().startswith("y"):  continue

			run_check(cur, lastModify, aLocn, begin_at_root)
			ch = input("Modify-Date Scan Complete. Press 'y' to continue to Create-Date Scan: ")
			if not ch.lower().startswith("y"):  continue

			run_check(cur, createDate, aLocn, begin_at_root)
			print("Create-Date Scan Complete.")

		conn.commit()

	return "Database Closed"

if __name__ == '__main__':
	main()
