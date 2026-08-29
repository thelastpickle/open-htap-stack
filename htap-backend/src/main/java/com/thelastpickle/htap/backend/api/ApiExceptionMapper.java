package com.thelastpickle.htap.backend.api;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import java.util.Map;

@Provider
public class ApiExceptionMapper implements ExceptionMapper<ApiException> {

    @Override
    public Response toResponse(ApiException failure) {
        return Response.status(failure.status())
                .type(MediaType.APPLICATION_JSON)
                .entity(Map.of("detail", failure.getMessage()))
                .build();
    }
}
