const http = require("http");
const { Pool } = require("pg");

const MAX_RETRIES = 10;
const RETRY_TIMEOUT_MS = 50;

const server = http.createServer(async (req, res) => {
	let match = null;

	const logMsg = ["Request", req.method, req.url];
	let body = null;
	try {
		if (req.method === "POST") {
			body = await getBody(req);
		}
	} catch (error) {
		logMsg.push("Error parsing body", error.message);
	}

	if (body) {
		logMsg.push("Body", JSON.stringify(body, null, 2));
	}

	if (
		req.method === "POST" &&
		(match = req.url.match(/\/clientes\/(\d+)\/transacoes/))
	) {
		const client = await getClient();

		try {
			const [, id] = match;
			if (!Number.isInteger(body.valor)) {
				logMsg.push("Error", "Valor não é um número inteiro");
				res.writeHead(400);
				res.end();
				return;
			}
			const response =
				body.tipo === "c"
					? await retryFn(() => insertCredito(client, id, body), 3)
					: await retryFn(() => insertDebito(client, id, body), 3);

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
			console.log(logMsg.join(":"));
			client.release();
		}
	} else if (
		req.method === "GET" &&
		(match = req.url.match(/\/clientes\/(\d+)\/extrato/))
	) {
		const client = await getClient();

		try {
			const [, id] = match;
			const { rows } = await client.query(
				`
				SELECT s.valor as saldo, c.limite, t.descricao, t.valor, t.tipo, t.realizada_em
				FROM clientes c
				LEFT JOIN saldos s ON c.id = s.cliente_id
				LEFT JOIN transacoes t ON t.cliente_id = s.cliente_id
				WHERE s.cliente_id = $1
				ORDER BY t.id DESC
				LIMIT 10
			`,
				[id]
			);

			if (!rows.length) {
				res.writeHead(404, { "Content-Type": "text/plain" });
				res.end();
				return;
			}

			const saldo = {
				total: rows[0].saldo,
				limite: rows[0].limite,
			};

			const ultimas_transacoes = rows.filter((r) => !!r.realizada_em);

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
			res.writeHead(400);
			res.end();
			return;
		} finally {
			console.log(logMsg.join(":"));
			client.release();
		}
	} else {
		res.writeHead(404, { "Content-Type": "text/plain" });
		res.write("404 Not Found");
		res.end();
	}
});

const retryFn = async (fn, retry = 0) => {
	try {
		return fn();
	} catch (error) {
		if ([404, 422].includes(error.code)) {
			return Promise.reject(error);
		}

		if (retry > MAX_RETRIES) {
			return Promise.reject(error);
		}

		const timeout = RETRY_TIMEOUT_MS * (retry + 1);
		await sleep(timeout);
		return retryFn(fn, retry + 1);
	}
};

const sleep = async (ms) => {
	return new Promise((resolve) => {
		setTimeout(resolve, ms);
	});
};

const insertCredito = async (client, id, body) => {
	const cliente = await getCliente(client, id);

	try {
		await client.query("BEGIN");
		const [, saldo] = await Promise.all([
			await client.query(
				"INSERT INTO transacoes (cliente_id, descricao, valor, tipo) VALUES ($1, $2, $3, $4)",
				[id, body.descricao, body.valor, "c"]
			),
			await client.query(
				"UPDATE saldos SET valor = valor + $1 WHERE cliente_id = $2 RETURNING *",
				[body.valor, id]
			),
		]);

		await client.query("COMMIT");

		const valorSaldo = saldo.rows[0].valor;

		return {
			limite: cliente.limite,
			saldo: valorSaldo,
		};
	} catch (error) {
		await client.query("ROLLBACK");
		return Promise.reject(error);
	}
};

const insertDebito = async (client, id, body) => {
	const cliente = await getCliente(client, id);

	const limite = cliente.limite;

	try {
		await client.query("BEGIN");
		const {
			rows: [row],
		} = await client.query(
			`SELECT s.id, s.valor
			  FROM saldos s
			WHERE s.cliente_id = $1
			     FOR UPDATE`,
			[id]
		);

		if (!row) {
			return Promise.reject({ code: 404 });
		}

		const { valor: valorAtual, id: saldoId } = row;

		const saldo = valorAtual - body.valor;

		if (saldo < -1000) {
			return Promise.reject({ code: 422 });
		}

		await Promise.all([
			await client.query(
				"INSERT INTO transacoes (cliente_id, descricao, valor, tipo) VALUES ($1, $2, $3, $4)",
				[id, body.descricao, body.valor, "d"]
			),
			await client.query("UPDATE saldos SET valor = $1 WHERE id = $2", [
				saldo,
				saldoId,
			]),
		]);

		await client.query("COMMIT");

		return {
			limite,
			saldo,
		};
	} catch (error) {
		await client.query("ROLLBACK");
		return Promise.reject(error);
	}
};

const clientesCache = {};
const getCliente = async (client, id) => {
	if (clientesCache[id]) {
		return clientesCache[id];
	}

	const { rows } = await client.query("SELECT * FROM clientes WHERE id = $1", [
		id,
	]);

	if (!rows.length) {
		return Promise.reject({ code: 404 });
	}

	clientesCache[id] = rows[0];
	return clientesCache[id];
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

const getClient = () => {
	return pool.connect();
};

const pool = new Pool({
	host: "db",
	// host: "172.28.0.1",
	port: 5432,
	user: "admin",
	password: "123",
	connectionTimeoutMillis: 0,
	idleTimeoutMillis: 0,
	database: "rinha",
});

// Define the port the server will listen on
const port = 3000;

// Start the server and listen on the defined port
server.listen(port, () => {
	console.log(`Server running at http://localhost:${port}/`);
});
