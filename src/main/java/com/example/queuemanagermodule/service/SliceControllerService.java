package com.example.queuemanagermodule.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class SliceControllerService {

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    @Value("${slice.controller.service.base-url}")
    private String sliceControllerBaseUrl;

    /**
     * Envía una solicitud de despliegue de slice al Slice Controller
     */
    public Map<String, Object> deploySlice(Map<String, Object> sliceConfig) {
        String url = sliceControllerBaseUrl + "/deploy-slice";
        log.info("Enviando solicitud de despliegue de slice a: {}", url);

        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(sliceConfig, headers);

        ResponseEntity<Map> response = restTemplate.exchange(
                url,
                HttpMethod.POST,
                request,
                Map.class
        );

        log.info("Respuesta de despliegue recibida: {}", response.getStatusCode());
        return response.getBody();
    }

    /**
     * Envía una solicitud para detener un slice
     */
    public Map<String, Object> stopSlice(String sliceId, Map<String, Object> requestData) {
        String url = sliceControllerBaseUrl + "/stop-slice/" + sliceId;
        log.info("Enviando solicitud para detener slice a: {}", url);

        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(requestData, headers);

        ResponseEntity<Map> response = restTemplate.exchange(
                url,
                HttpMethod.POST,
                request,
                Map.class
        );

        log.info("Respuesta de detención recibida: {}", response.getStatusCode());
        return response.getBody();
    }

    /**
     * Envía una solicitud para reiniciar un slice
     */
    public Map<String, Object> restartSlice(String sliceId, Map<String, Object> requestData) {
        String url = sliceControllerBaseUrl + "/restart-slice/" + sliceId;
        log.info("Enviando solicitud para reiniciar slice a: {}", url);

        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(requestData, headers);

        ResponseEntity<Map> response = restTemplate.exchange(
                url,
                HttpMethod.POST,
                request,
                Map.class
        );

        log.info("Respuesta de reinicio recibida: {}", response.getStatusCode());
        return response.getBody();
    }

    /**
     * Envía una solicitud para pausar una VM
     */
    public Map<String, Object> pauseVm(String vmId, Map<String, Object> requestData) {
        String url = sliceControllerBaseUrl + "/pause-vm/" + vmId;
        log.info("Enviando solicitud para pausar VM a: {}", url);

        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(requestData, headers);

        ResponseEntity<Map> response = restTemplate.exchange(
                url,
                HttpMethod.POST,
                request,
                Map.class
        );

        log.info("Respuesta de pausa recibida: {}", response.getStatusCode());
        return response.getBody();
    }

    /**
     * Envía una solicitud para reanudar una VM
     */
    public Map<String, Object> resumeVm(String vmId, Map<String, Object> requestData) {
        String url = sliceControllerBaseUrl + "/resume-vm/" + vmId;
        log.info("Enviando solicitud para reanudar VM a: {}", url);

        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(requestData, headers);

        ResponseEntity<Map> response = restTemplate.exchange(
                url,
                HttpMethod.POST,
                request,
                Map.class
        );

        log.info("Respuesta de reanudación recibida: {}", response.getStatusCode());
        return response.getBody();
    }

    /**
     * Envía una solicitud para reiniciar una VM
     */
    public Map<String, Object> restartVm(String vmId, Map<String, Object> requestData) {
        String url = sliceControllerBaseUrl + "/restart-vm/" + vmId;
        log.info("Enviando solicitud para reiniciar VM a: {}", url);

        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(requestData, headers);

        ResponseEntity<Map> response = restTemplate.exchange(
                url,
                HttpMethod.POST,
                request,
                Map.class
        );

        log.info("Respuesta de reinicio recibida: {}", response.getStatusCode());
        return response.getBody();
    }

    /**
     * Envía una solicitud para generar un token VNC
     */
    public Map<String, Object> generateVncToken(String vmId, Map<String, Object> requestData) {
        String url = sliceControllerBaseUrl + "/vm-token/" + vmId;
        log.info("Enviando solicitud para generar token VNC a: {}", url);

        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(requestData, headers);

        ResponseEntity<Map> response = restTemplate.exchange(
                url,
                HttpMethod.POST,
                request,
                Map.class
        );

        log.info("Respuesta de generación de token recibida: {}", response.getStatusCode());
        return response.getBody();
    }

    /**
     * Envía una solicitud para sincronizar imágenes
     */
    public Map<String, Object> syncImages(Map<String, Object> requestData) {
        String url = sliceControllerBaseUrl + "/sync-images";
        log.info("Enviando solicitud para sincronizar imágenes a: {}", url);

        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(requestData, headers);

        ResponseEntity<Map> response = restTemplate.exchange(
                url,
                HttpMethod.POST,
                request,
                Map.class
        );

        log.info("Respuesta de sincronización recibida: {}", response.getStatusCode());
        return response.getBody();
    }
}