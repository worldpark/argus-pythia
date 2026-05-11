package com.example.pythia.email.dto;

import java.util.List;

public record EmailRequest(
    List<String> to,
    String subject,
    String body) {
}
