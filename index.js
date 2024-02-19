const http = require("http");
const { Pool } = require("pg");

const server = http.createServer(async (req, res) => {
	let match = null;

	res.on("close", () => {
		if (res.statusCode === 500) {
			console.log(logMsg.join(":"));
		}
	});

	const logMsg = [req.method, req.url];
	let body = null;
	try {
		if (req.method === "POST") {
			body = await getBody(req);
		}
	} catch (error) {
		logMsg.push("Error parsing body", error.message);
	}

	if (body) {
		logMsg.push("Body", JSON.stringify(body));
	}

	if (
		req.method === "POST" &&
		(match = req.url.match(/\/clientes\/(\d+)\/transacoes/))
	) {
		const client = await getClient();

		try {
			const [, id] = match;
			if (!validarTransacao(body)) {
				logMsg.push("Error", "Transacao inválida");
				res.writeHead(422);
				res.end();
				return;
			}
			const response =
				body.tipo === "c"
					? await insertCredito(client, id, body)
					: await insertDebito(client, id, body);

			res.writeHead(200, { "Content-Type": "application/json" });

			try {
				res.write(JSON.stringify(response));
			} catch (error) {
				console.log({ responseVazio: true, error });
			}

			res.end();
			return;
		} catch (error) {
			let code = parseInt(error.code);
			if (error.code === "25P02") {
				code = 504;
			} else if (error.code === "40P01") {
				code = 503;
			}

			if (Number.isNaN(code) || code < 400 || code > 499) {
				code = 500;
			}

			logMsg.push("ErrorCode", error.code);
			logMsg.push("Error", JSON.stringify(error, null, 2));
			res.writeHead(code, { "Content-Type": "text/plain" });
			res.end();
			return;
		} finally {
			client.release();
		}
	} else if (
		req.method === "GET" &&
		(match = req.url.match(/\/clientes\/(\d+)\/extrato/))
	) {
		const client = await getClient();
		const [, id] = match;

		try {
			const { rows } = await client.query(
				`SELECT t.id as id_transacao, c.saldo, c.limite, t.descricao, t.valor, t.tipo, t.realizada_em
				FROM clientes c
				LEFT JOIN transacoes t ON t.cliente_id = c.id
				WHERE c.id = $1
				ORDER BY t.id DESC
				LIMIT 11`,
				[id]
			);

			if (!rows.length) {
				res.writeHead(404, { "Content-Type": "text/plain" });
				res.end();
				return;
			}

			if (rows.length > 10) {
				cleanUp(id, rows[10].id_transacao);
			}

			const saldo = {
				total: rows[0].saldo,
				limite: rows[0].limite,
				data_extrato: new Date().toISOString(),
			};

			const ultimas_transacoes = !rows?.[0]?.realizada_em
				? []
				: rows.slice(0, 10);

			const response = {
				saldo,
				ultimas_transacoes,
			};

			res.writeHead(200, { "Content-Type": "application/json" });
			try {
				res.write(JSON.stringify(response));
			} catch (error) {
				console.log({ responseVazio: true, error });
			}
			res.end();
			return;
		} catch (error) {
			logMsg.push("ErrorCode", error.code);
			logMsg.push("Error", JSON.stringify(error, null, 2));
			res.writeHead(error.code === 404 ? 404 : 500);
			res.end();
			return;
		} finally {
			client.release();
		}
	} else {
		res.writeHead(404, { "Content-Type": "text/plain" });
		res.write("404 Not Found");
		res.end();
	}
});

const validarTransacao = (body) => {
	if (!Number.isInteger(body.valor)) {
		return false;
	}

	if (!["c", "d"].includes(body.tipo)) {
		return false;
	}

	if (!body.descricao) {
		return false;
	}

	const descricao = body.descricao.replace(/[^a-zA-Z0-9]/g, "");
	if (descricao.length > 10) {
		return false;
	}

	return true;
};

const insertCredito = async (client, id, body) => {
	try {
		const {
			rows: [row],
		} = await client.query(`SELECT creditar($1, $2, $3) as result`, [
			id,
			body.valor,
			body?.descricao ?? "",
		]);

		if (!row || typeof row?.result !== "string") {
			return Promise.reject({ code: 400 });
		}

		const [novo_saldo, limite] = row.result
			.replace("(", "")
			.replace(")", "")
			.split(",");

		return {
			limite: parseInt(limite),
			saldo: parseInt(novo_saldo),
		};
	} catch (error) {
		return Promise.reject(error);
	}
};

const mapCleanUp = {};
const cleanUp = (client_id, id_transacao) => {
	if (!id_transacao) {
		return;
	}

	if (mapCleanUp[client_id]) {
		clearTimeout(mapCleanUp[client_id]);
	}

	mapCleanUp[client_id] = setTimeout(() => {
		delete mapCleanUp[client_id];
		pool.query(`DELETE FROM transacoes WHERE id <= $1 AND cliente_id = $2`, [
			id_transacao,
			client_id,
		]);
	}, 1000);
};

const insertDebito = async (client, id, body) => {
	try {
		const {
			rows: [row],
		} = await client.query(`SELECT debitar($1, $2, $3) as result`, [
			id,
			body.valor,
			body?.descricao ?? "",
		]);

		if (!row || typeof row?.result !== "string") {
			return Promise.reject({ code: 400 });
		}

		const [sucesso, novo_saldo, limite] = row.result
			.replace("(", "")
			.replace(")", "")
			.split(",");

		if (sucesso === "0") {
			return Promise.reject({ code: 422 });
		}

		return {
			limite: parseInt(limite),
			saldo: parseInt(novo_saldo),
		};
	} catch (error) {
		return Promise.reject(error);
	}
};

const getBody = (req) => {
	return new Promise((resolve, reject) => {
		let body = "";
		req.on("data", (chunk) => {
			body += chunk.toString();
		});

		req.on("end", () => {
			try {
				resolve(JSON.parse(body));
			} catch (error) {
				reject(error);
			}
		});
	});
};

const getClient = async () => {
	const client = await pool.connect();

	return client;
};

const pool = new Pool({
	host: process.env.DB_HOSTNAME,
	port: 5432,
	user: "admin",
	password: "123",
	connectionTimeoutMillis: 60_000,
	idleTimeoutMillis: 0,
	max: process.env.DB_MAX_POOL_SIZE,
	database: "rinha",
});

const port = process.env.PORT;

server.listen(port, () => {
	console.log(`Server running at http://localhost:${port}/`);
	Promise.all(
		Array(process.env.DB_INITIAL_POOL_SIZE)
			.fill()
			.map(() => getClient())
	).then((clients) => {
		clients.forEach((client) => client.release());
		console.log("Pool de conexões criado com sucesso");
	});
});
