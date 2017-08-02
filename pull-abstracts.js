const crypto = require('crypto');
const mysql = require('mysql2/promise');
const sqlite3 = require('sqlite');
const config = require('./config');

async function getLibraries(connection) {
	let sql = 'SELECT libraryID, version FROM shardLibraries';
	let [rows] = await connection.execute(sql, []);
	return rows;
}

async function getLibraryData(connection, libraryID, version) {
	let sql = `
	SELECT itd1.value AS abstract,
	       itd2.value AS doi,
	       itd3.value AS issn
	FROM items AS itm
	LEFT JOIN itemData AS itd1 ON (itd1.itemID = itm.itemID AND itd1.fieldID=90)
	LEFT JOIN itemData AS itd2 ON (itd2.itemID = itm.itemID AND itd2.fieldID=26 )
	LEFT JOIN itemData AS itd3 ON (itd3.itemID = itm.itemID AND itd3.fieldID=13 )
	WHERE itm.libraryID = ?
	AND itm.version >= ?
	AND itd1.value IS NOT NULL
	AND (
				itd2.value IS NOT NULL OR
				itd3.value IS NOT NULL
			)
	`;
	
	let [rows] = await connection.execute(sql, [libraryID, version]);
	return rows;
}

async function getLocalLibraryVersion(db, libraryID) {
	let res = await db.get('SELECT version FROM libraries WHERE libraryID = ?', [libraryID]);
	if (!res) return null;
	return res.version;
}

async function setLocalLibraryVersion(db, libraryID, version) {
	let res = await db.run('INSERT OR REPLACE INTO libraries (libraryID, version) VALUES (?,?)',
		[libraryID, version]);
}

async function getSnippetByHash(db, hash) {
	let res = await db.get('SELECT id, identifiers FROM snippets WHERE hash = ?', [hash]);
	if (!res) return null;
	return res.id;
}

async function insertSnippet(db, hash, identifiers, text) {
	let res = await db.run('INSERT INTO snippets (hash, identifiers, text) VALUES (?,?,?)',
		[hash, identifiers, text]);
}

async function updateIdentifiers(db, id, identifiers) {
	let res = await db.run('UPDATE snippet SET identifiers = ? WHERE id = ?',
		[identifiers, id]);
}

function combineIdentifiers(identifiers, item) {
	if (item.doi) {
		if (identifiers.doi) {
			if (!identifiers.doi.includes(item.doi)) {
				identifiers.doi.push(item.doi);
			}
		}
		else {
			identifiers.doi = [];
			identifiers.doi.push(item.doi);
		}
	}
	
	if (item.isbn) {
		if (identifiers.isbn) {
			if (!identifiers.isbn.includes(item.isbn)) {
				identifiers.isbn.push(item.isbn);
			}
		}
		else {
			identifiers.isbn = [];
			identifiers.isbn.push(item.isbn);
		}
	}
	// Todo: add more ids;don't update sqlite if no new id is added
}

async function main() {
	const db = await sqlite3.open("./db.sqlite", {Promise});
	await db.run("CREATE TABLE IF NOT EXISTS libraries (libraryID INTEGER PRIMARY KEY, version INTEGER)");
	await db.run("CREATE TABLE IF NOT EXISTS snippets (id INTEGER PRIMARY KEY, hash VARCHAR(32), identifiers TEXT, text TEXT)");
	await db.run("CREATE UNIQUE INDEX IF NOT EXISTS hash_index ON snippets (hash)");
	
	const master = await mysql.createConnection({
		host: '172.13.0.4',
		user: 'root',
		database: 'zoterotest_master'
	});
	// query database
	const [shardRows] = await master.execute(
		'SELECT * FROM shards AS s LEFT JOIN shardHosts AS sh USING(shardHostID)',
		[]
	);
	
	master.close();
	
	for (let i = 0; i < shardRows.length; i++) {
		let shardRow = shardRows[i];
		
		try {
			let connection = await mysql.createConnection({
				host: '172.13.0.4',
				user: 'root',
				database: shardRow.db
			});
			
			let libraries = await getLibraries(connection);
			
			for (let j = 0; j < libraries.length; j++) {
				let library = libraries[j];
				
				let version = await getLocalLibraryVersion(db, library.libraryID);
				
				if (library.version > version) {
					let items = await getLibraryData(connection, library.libraryID, version + 1);
					
					for (let k = 0; k < items.length; k++) {
						let item = items[k];
						
						let hash = crypto.createHash('md5').update(item.abstract).digest("hex");
						
						let snippet = await getSnippetByHash(db, hash);
						if (snippet) {
							let identifiers = JSON.parse(snippet.identifiers);
							
							combineIdentifiers(identifiers, item);
							
							identifiers = JSON.stringify(identifiers);
							updateIdentifiers(db, snippet.id, identifiers);
						}
						else {
							let identifiers = {};
							combineIdentifiers(identifiers, item);
							let abstract = item.abstract.slice(0, 8192);
							identifiers = JSON.stringify(identifiers);
							insertSnippet(db, hash, identifiers, abstract);
						}
					}
					await setLocalLibraryVersion(db, library.libraryID, library.version);
				}
			}
			connection.close();
		}
		catch (err) {
			console.log(err);
		}
	}
}

main();


