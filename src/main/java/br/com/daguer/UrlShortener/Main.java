package br.com.daguer.UrlShortener;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Classe principal que implementa o manipulador de requisições AWS Lambda.
 */

public class Main implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private final S3Client s3Client = S3Client.builder().build();
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Manipula a requisição recebida pela AWS Lambda.
     *
     * @param input   O mapa de entrada contendo os parâmetros da requisição.
     * @param context O contexto da execução da Lambda.
     * @return Um mapa contendo o código de status HTTP e o corpo da resposta.
     */

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> input, Context context) {

        // Obtém o código da URL encurtada a partir dos parâmetros do caminho.
        String pathParameters = (String) input.get("rawPath");
        String shortUrlCode = pathParameters.replace("/", "");

        // Verifica se o código da URL encurtada é válido.
        if (shortUrlCode == null || shortUrlCode.isEmpty()) {
            throw new IllegalArgumentException("Invalid short URL code");
        }

        // Cria uma requisição para obter o objeto do S3.
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket("daguer-url-shortener-storage")
                .key(shortUrlCode + ".json")
                .build();

        InputStream s3ObjectStream;

        // Tenta obter o objeto do S3.
        try {
            s3ObjectStream = s3Client.getObject(getObjectRequest);
        } catch (Exception exception) {
            context.getLogger().log("Erro ao buscar o objeto do S3: " + exception.getMessage());
            throw new IllegalArgumentException("Error while fetching short URL! " + exception.getMessage(), exception);
        }

        UrlData urlData;

        // Tenta desserializar os dados da URL a partir do objeto do S3.
        try {
            urlData = objectMapper.readValue(s3ObjectStream, UrlData.class);
        } catch (Exception exception) {
            throw new RuntimeException("Error deserializing URL data: " + exception.getMessage(), exception);
        }

        long CurrentTimeInSeconds = System.currentTimeMillis() / 1000;

        Map<String, Object> response = new HashMap<>();

        // Verifica se a URL expirou.
        if (urlData.getExpirationTime() < CurrentTimeInSeconds) {
            response.put("statusCode", 404);
            response.put("body", "This URL has expired");
            return response;
        }

        // Retorna a resposta com redirecionamento para a URL original.
        response.put("statusCode", 302);
        Map<String, String> headers = new HashMap<>();
        headers.put("Location", urlData.getOriginalUrl());
        response.put("headers", headers);

        return response;
    }
}