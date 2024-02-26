<?php

use Swoole\Http\Request;
use Swoole\Http\Response;

// Create an HTTP server
$http = new Swoole\Http\Server("0.0.0.0", 8000);

// Configure PostgreSQL connection
$pgConfig = [
    'host' => getenv('DB_HOSTNAME'),
    'port' => 5432,
    'user' => 'admin',
    'password' => '123',
    'dbname' => 'rinha',
];

// Event handler for handling requests
$http->on("request", function (Request $request, Response $response) use ($pgConfig) {
    $uri = $request->server['request_uri'];
    $method = $request->server['request_method'];
    $body = $request->rawContent();

    $logMsg = [$method, $uri];

    $response->on("close", function () use ($response, $logMsg) {
        if ($response->getStatusCode() === 500) {
            echo implode(':', $logMsg);
        }
    });

    try {
        if ($method === 'POST') {
            // Parse JSON body
            $jsonBody = json_decode($body, true);
            if ($jsonBody === null && json_last_error() !== JSON_ERROR_NONE) {
                throw new Exception("Error parsing JSON body");
            }
        }

        if (preg_match("#/clientes/(\d+)/transacoes#", $uri, $matches) && $method === 'POST') {
            $id = $matches[1];
            if (!validarTransacao($jsonBody)) {
                $logMsg[] = "Error: Transacao invalida";
                $response->status(422);
                $response->end();
                return;
            }

            $client = new Swoole\Coroutine\Postgres();
            $client->connect($pgConfig, function ($client, $result) use ($id, $jsonBody, $response, $logMsg) {
                if ($result === false) {
                    $response->status(500);
                    $response->end();
                    return;
                }

                $query = $jsonBody['tipo'] === 'c' ? "SELECT creditar($1, $2, $3)" : "SELECT debitar($1, $2, $3)";
                $client->queryParams($query, [$id, $jsonBody['valor'], $jsonBody['descricao']], function ($client, $result) use ($response, $logMsg) {
                    if ($result === false) {
                        $response->status(500);
                        $response->end();
                        return;
                    }

                    $row = pg_fetch_assoc($result);
                    if ($row === false || !isset($row['result'])) {
                        $response->status(400);
                        $response->end();
                        return;
                    }

                    $resultParts = explode(',', trim($row['result'], '()'));
                    $response->header('Content-Type', 'application/json');
                    $response->write(json_encode([
                        'limite' => (int)$resultParts[1],
                        'saldo' => (int)$resultParts[0]
                    ]));
                    $response->end();
                });
            });
        } elseif (preg_match("#/clientes/(\d+)/extrato#", $uri, $matches) && $method === 'GET') {
            $id = $matches[1];
            $client = new Swoole\Coroutine\Postgres();
            $client->connect($pgConfig, function ($client, $result) use ($id, $response) {
                if ($result === false) {
                    $response->status(500);
                    $response->end();
                    return;
                }

                $query = "SELECT t.id as id_transacao, c.saldo, c.limite, t.descricao, t.valor, t.tipo, t.realizada_em FROM clientes c LEFT JOIN transacoes t ON t.cliente_id = c.id WHERE c.id = $1 ORDER BY t.id DESC LIMIT 11";
                $client->queryParams($query, [$id], function ($client, $result) use ($response) {
                    if ($result === false) {
                        $response->status(500);
                        $response->end();
                        return;
                    }

                    $rows = pg_fetch_all($result);
                    if ($rows === false || empty($rows)) {
                        $response->status(404);
                        $response->end();
                        return;
                    }

                    if (count($rows) > 10) {
                        cleanUp($id, $rows[10]['id_transacao']);
                    }

                    $saldo = [
                        'total' => (int)$rows[0]['saldo'],
                        'limite' => (int)$rows[0]['limite'],
                        'data_extrato' => date('c')
                    ];

                    $ultimas_transacoes = [];
                    if (!empty($rows[0]['realizada_em'])) {
                        $ultimas_transacoes = array_slice($rows, 0, 10);
                    }

                    $response->header('Content-Type', 'application/json');
                    $response->write(json_encode([
                        'saldo' => $saldo,
                        'ultimas_transacoes' => $ultimas_transacoes
                    ]));
                    $response->end();
                });
            });
        } else {
            $response->status(404);
            $response->end("404 Not Found");
        }
    } catch (Exception $e) {
        $logMsg[] = "Error: " . $e->getMessage();
        $response->status(500);
        $response->end();
    }
});

// Start the server
$http->start();

function validarTransacao($body)
{
    if (!isset($body['valor']) || !is_int($body['valor'])) {
        return false;
    }

    if (!isset($body['tipo']) || !in_array($body['tipo'], ['c', 'd'])) {
        return false;
    }

    if (!isset($body['descricao']) || strlen(preg_replace('/[^a-zA-Z0-9]/', '', $body['descricao'])) > 10) {
        return false;
    }

    return true;
}

function cleanUp($client_id, $id_transacao)
{
    if (empty($id_transacao)) {
        return;
    }

    $timerId = swoole_timer_after(1000, function () use ($client_id, $id_transacao) {
        $pgConfig = [
            'host' => getenv('DB_HOSTNAME'),
            'port' => 5432,
            'user' => 'admin',
            'password' => '123',
            'dbname' => 'rinha',
        ];

        $client = new Swoole\Coroutine\Postgres();
        $client->connect($pgConfig, function ($client, $result) use ($client_id, $id_transacao) {
            if ($result === false) {
                return;
            }

            $client->queryParams("DELETE FROM transacoes WHERE id <= $1 AND cliente_id = $2", [$id_transacao, $client_id]);
        });
    });
}