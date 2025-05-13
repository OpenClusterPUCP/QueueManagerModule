package com.example.queuemanagermodule.model;

public enum OperationType {
    DEPLOY_SLICE,
    STOP_SLICE,
    RESTART_SLICE,
    PAUSE_VM,
    RESUME_VM,
    RESTART_VM,
    GENERATE_VNC_TOKEN,
    SYNC_IMAGES
}