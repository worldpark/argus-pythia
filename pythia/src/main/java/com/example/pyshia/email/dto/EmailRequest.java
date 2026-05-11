package com.example.pyshia.email.dto;

import java.util.List;

public record EmailRequest(
    List<String> to,
    String subject,
    String body) {
}
