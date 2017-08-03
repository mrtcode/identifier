/*
 ***** BEGIN LICENSE BLOCK *****
 
 This file is part of the Zotero Data Server.
 
 Copyright © 2017 Center for History and New Media
 George Mason University, Fairfax, Virginia, USA
 http://zotero.org
 
 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.
 
 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 
 ***** END LICENSE BLOCK *****
 */

const request = require('request');
const sqlite3 = require('sqlite');

function index(batch) {
	return new Promise(function(resolve, reject) {
		request({
			url: 'http://localhost:8001/index',
			method: 'POST',
			json: batch,
		}, function (err, res) {
			if(err) return reject(err);
			resolve(res.body);
		});
	});
}

async function main() {
	const db = await sqlite3.open('./db.sqlite', {Promise});
	
	let stmt = await db.prepare("SELECT id, text FROM snippets");
	
	let batch = [];
	let row;
	while((row = await stmt.get())) {
		console.log(row);
		batch.push({
			id: row.id,
			text: row.text
		});
		
		if(batch.length>=1000) {
			await index(batch);
			batch = [];
		}
	}
	
	await index(batch);
}

main();