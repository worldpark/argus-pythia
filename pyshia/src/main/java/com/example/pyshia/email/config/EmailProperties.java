package com.example.pyshia.email.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "pyshia.email")
public record EmailProperties(
    @NotBlank String from,
    @NotEmpty List<String> operatorRecipients) {
}
