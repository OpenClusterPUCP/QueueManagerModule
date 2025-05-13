package com.example.queuemanagermodule.model;

public enum OperationStatus {
    PENDING,         // En cola, aún no procesada
    IN_PROGRESS,     // Está siendo procesada actualmente
    COMPLETED,       // Completada exitosamente
    FAILED,          // Falló durante la ejecución
    TIMEOUT,         // No se completó dentro del tiempo establecido
    CANCELLED        // Cancelada manualmente
}
