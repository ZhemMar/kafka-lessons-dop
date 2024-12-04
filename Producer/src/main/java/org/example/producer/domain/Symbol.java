package org.example.producer.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Symbol {
    private Long id;
    private String value;
    private String color;
    private String type;
}
