const http = require("http");
const { Pool } = require("pg");

const server = http.createServer(async (req, res) => {
  let match = null;

  if (
    req.method === "POST" &&
    (match = req.url.match(/\/clientes\/(\d+)\/transacoes/))
  ) {
    const client = await getClient();

    try {
      const [, id] = match;
      const body = await getBody(req);
      const response = await insertTransacao(client, id, body);

      res.writeHead(200, { "Content-Type": "application/json" });

      res.write(JSON.stringify(response));

      res.end();
      return;
    } catch (error) {
      res.writeHead(error.code, { "Content-Type": "text/plain" });
      res.end();
      return;
    } finally {
      client.release();
    }
  }

  if (
    req.method === "GET" &&
    (match = req.url.match(/\/clientes\/(\d+)\/extrato/))
  ) {
    const client = await getClient();

    try {
      const [, id] = match;
      const { rows } = await client.query(
        `
		SELECT s.valor as saldo, c.limite, t.descricao, t.valor, t.tipo, t.realizada_em
		FROM saldos s
		JOIN clientes c ON c.id = s.cliente_id
		JOIN transacoes t ON t.cliente_id = s.cliente_id
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

      const ultimas_transacoes = rows;

      const response = {
        saldo,
        ultimas_transacoes,
      };

      res.writeHead(200, { "Content-Type": "application/json" });
      res.write(JSON.stringify(response));
      res.end();
      return;
    } catch (error) {
      res.writeHead(400, { "Content-Type": "text/plain" });
      res.end();
      return;
    } finally {
      client.release();
    }
  }

  res.writeHead(404, { "Content-Type": "text/plain" });
  res.write("404 Not Found");
  res.end();
});

const insertTransacao = async (client, id, body) => {
  try {
    await client.query("BEGIN");
    const {
      rows: [row],
    } = await client.query(
      `SELECT s.id, s.valor, c.limite
	  	FROM saldos s
		  JOIN clientes c ON c.id = s.cliente_id
	  WHERE cliente_id = $1
	  FOR UPDATE`,
      [id]
    );

    if (!row) {
      return Promise.reject({ code: 404 });
    }

    const { valor: valorAtual, id: saldoId, limite } = row;

    const saldo =
      body.tipo === "d" ? valorAtual - body.valor : valorAtual + body.valor;

    if (saldo < -1000) {
      return Promise.reject({ code: 422 });
    }

    await Promise.all([
      await client.query(
        "INSERT INTO transacoes (cliente_id, descricao, valor, tipo) VALUES ($1, $2, $3, $4)",
        [id, body.descricao, body.valor, body.tipo]
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
    throw error;
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

const getClient = () => {
  return pool.connect();
};

const pool = new Pool({
  host: "db",
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
