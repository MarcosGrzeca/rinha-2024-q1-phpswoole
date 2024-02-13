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
      res.writeHead(400, { "Content-Type": "text/plain" });
      res.write(error.message);
      res.end();
      return;
    } finally {
      client.release();
    }
  }

  // If the request is not for '/balance', respond with a 404 Not Found
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
      throw new Error("Cliente não encontrado");
    }

    const { valor: valorAtual, id: saldoId, limite } = row;

    const valor =
      body.tipo === "d" ? valorAtual - body.valor : valorAtual + body.valor;

    await client.query(
      "INSERT INTO transacoes (cliente_id, descricao, valor, tipo) VALUES ($1, $2, $3, $4)",
      [id, body.descricao, body.valor, body.tipo]
    );

    await client.query("UPDATE saldos SET valor = $1 WHERE id = $2", [
      valor,
      saldoId,
    ]);

    await client.query("COMMIT");

    return {
      limite,
      saldo: valor,
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
