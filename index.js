const http = require("http");
const { Pool } = require("pg");

const MAX_RETRIES = 10;
const RETRY_TIMEOUT_MS = 50;

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
			client.release();
		}
	} else if (
		req.method === "GET" &&
		(match = req.url.match(/\/clientes\/(\d+)\/extrato/))
	) {
		const client = await getClient();
		const [, id] = match;

		try {
			const cliente = await getCliente(client, id);
			const { rows } = await client.query(
				`SELECT c.saldo, c.limite, t.descricao, t.valor, t.tipo, t.realizada_em
				FROM clientes c
				LEFT JOIN transacoes t ON t.cliente_id = c.id
				WHERE c.id = $1
				ORDER BY t.id DESC
				LIMIT 10`,
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

			const ultimas_transacoes = !rows?.[0]?.realizada_em ? [] : rows;

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

		const [novo_saldo, possui_erro, mensagem] = row.result
			.replace("(", "")
			.replace(")", "")
			.split(",");

		return {
			limite: cliente.limite,
			saldo: parseInt(novo_saldo),
		};
	} catch (error) {
		return Promise.reject(error);
	}
};

const insertDebito = async (client, id, body) => {
	const cliente = await getCliente(client, id);

	const limite = cliente.limite;

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

		const [novo_saldo, possui_erro, mensagem] = row.result
			.replace("(", "")
			.replace(")", "")
			.split(",");

		if (possui_erro === "t") {
			return Promise.reject({ code: 422 });
		}

		return {
			limite,
			saldo: parseInt(novo_saldo),
		};
	} catch (error) {
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
	// host: "db",
	host: "172.28.0.1",
	port: 5432,
	user: "admin",
	password: "123",
	connectionTimeoutMillis: 0,
	idleTimeoutMillis: 0,
	max: 100,
	database: "rinha",
});

// Define the port the server will listen on
const port = 3000;

// Start the server and listen on the defined port
server.listen(port, () => {
	console.log(`Server running at http://localhost:${port}/`);
});
