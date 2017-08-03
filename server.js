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

const Koa = require('koa');
const Router = require('koa-router');
const bodyParser = require('koa-bodyparser');
const request = require('request');
const sqlite = require('sqlite');
const config = require('./config');

const app = new Koa();
const router = new Router();


/**
 * A middleware to catch all errors
 */
app.use(async function (ctx, next) {
	try {
		await next()
	} catch (err) {
		ctx.status = err.status || 500;
		if (err.expose) {
			ctx.message = err.message;
		}
		
		// Be verbose only with internal server errors
		if (err.status) {
			console.log(err.message);
		} else {
			console.log(err);
		}
	}
});

app.use(bodyParser());

function identify_snippets(text) {
	return new Promise(function(resolve, reject) {
		request({
			url: 'http://localhost:8001/identify',
			method: 'POST',
			json: {
				text
			},
		}, function (err, res) {
			if(err) return reject(err);
			resolve(res.body.ids);
		});
	});
}

async function identify(text) {
	let snippets = await identify_snippets(text);
	let result = {
		doi: [],
		isbn: []
	};
	
	let max_fingerprints = -1;
	for(let i=0;i<snippets.length;i++) {
		let snippet = snippets[i];
		if(snippet.count>=2) {
			max_fingerprints = snippet.count;
			let row = await db.get('SELECT identifiers FROM snippets WHERE id = ?', [snippet.id]);
			
			let identifiers = JSON.parse(row.identifiers);
			
			if(identifiers.doi) {
				for(let j=0;j<identifiers.doi.length;j++) {
					let doi = identifiers.doi[j];
					if(!result.doi.includes(doi)) { // Todo: case insensitive?
						result.doi.push(doi);
					}
				}
			}
			
			if(identifiers.isbn) {
				for(let j=0;j<identifiers.isbn.length;j++) {
					let isbn = identifiers.isbn[j];
					if(!result.isbn.includes(isbn)) { // Todo: case insensitive?
						result.isbn.push(isbn);
					}
				}
			}
			
		}
	}
	
	return result;
}


router.post('/identify', async function (ctx) {
	console.log(ctx.request.body);
	let text = ctx.request.body.text;
	ctx.body = await identify(text);
});

router.get('/identify', async function (ctx) {
	ctx.body = `
	<html>
		<head>
			<title>PDF Identifier</title>
		</head>
		<body>
			<p>
				<button id="identify">Identify</button>
			</p>
			<p>
				<textarea id="text" style="width:600px;height:300px;"></textarea>
			</p>
			<p id="result">
			</p>
		</body>
		<script>
			document.getElementById('identify').addEventListener('click', function() {
				fetch('/identify', {
		      method: "POST",
				  headers: {
				    'Accept': 'application/json, text/plain, */*',
				    'Content-Type': 'application/json'
				  },
				  body: JSON.stringify({
				    text: document.getElementById('text').value
				  })
				})
				.then(function(res){ return res.json(); })
				.then(function(data){
					
					var result = document.getElementById('result');
					
					result.innerHTML = '';
					
					for(var i=0;i<data.doi.length;i++) {
						var doi = data.doi[i];
						var a = document.createElement('a');
						a.href = 'http://dx.doi.org/'+doi;
						a.target = 'blank';
						var text = document.createTextNode(doi);
						a.appendChild(text);
						result.appendChild(a);
						var br = document.createElement('br');
						result.appendChild(br);
					}
					
				  for(var i=0;i<data.isbn.length;i++) {
						var isbn = data.isbn[i];
						var a = document.createElement('a');
						a.href = 'https://www.bookfinder.com/search/?isbn='+isbn;
						a.target = 'blank';
						var text = document.createTextNode(isbn);
						a.appendChild(text);
						result.appendChild(a);
						var br = document.createElement('br');
						result.appendChild(br);
					}
					
				})
			});
		</script>
	</html>
	`;
});

app
	.use(router.routes())
	.use(router.allowedMethods());


// Client connection errors
app.on('error', function (err, ctx) {
	//log.debug('App error: ', err, ctx);
});


let db = null;
let server = null;
async function main() {
	db = await sqlite.open('./db.sqlite', {Promise});
	server = app.listen(8003);
}

main();
