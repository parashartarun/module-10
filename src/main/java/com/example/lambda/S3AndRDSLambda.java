package com.example.lambda;

    import com.amazonaws.services.lambda.runtime.Context;
    import com.amazonaws.services.lambda.runtime.RequestHandler;
    import software.amazon.awssdk.services.s3.S3Client;
    import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
    import software.amazon.awssdk.services.s3.model.S3Object;

    import java.sql.Connection;
    import java.sql.DriverManager;
    import java.sql.ResultSet;
    import java.sql.Statement;
    import java.util.*;
    import java.util.stream.Collectors;

    public class S3AndRDSLambda implements RequestHandler<Map<String, Object>, Map<String, Object>> {

        private final S3Client s3 = S3Client.create();
        private final Connection dbConnection;
        private final String tableName;

        public S3AndRDSLambda() {
            try {
                String jdbcUrl = System.getenv("DB_URL");
                String dbUser = System.getenv("DB_USER");
                String dbPassword = System.getenv("DB_PASSWORD");
                this.tableName = System.getenv("DB_TABLE");

                this.dbConnection = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword);
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize DB connection", e);
            }
        }

        @Override
        public Map<String, Object> handleRequest(Map<String, Object> event, Context context) {
            String bucketName = System.getenv("BUCKET_NAME");
            String detailType = (String) event.get("detail-type");
            context.getLogger().log("DetailType: " + detailType + "\n");

            context.getLogger().log("Reading from bucket: " + bucketName + "\n");
            context.getLogger().log("Reading from table: " + tableName + "\n");

            try {
                Set<String> s3Keys = listS3Objects(bucketName);
                List<ImageDto> dbKeys = fetchImages();

                Set<String> dbNames = dbKeys.stream()
                        .map(ImageDto::getName)
                        .collect(Collectors.toSet());

                Set<String> missingInDb = new HashSet<>(s3Keys);
                missingInDb.removeAll(dbNames);

                Set<String> missingInS3 = new HashSet<>(dbNames);
                missingInS3.removeAll(s3Keys);

                if (missingInDb.isEmpty() && missingInS3.isEmpty()) {
                    context.getLogger().log("✅ No differences found between S3 and RDS.\n");
                } else {
                    if (!missingInDb.isEmpty()) {
                        context.getLogger().log("⚠️ Present in S3 but missing in DB: " + missingInDb + "\n");
                    }
                    if (!missingInS3.isEmpty()) {
                        context.getLogger().log("⚠️ Present in DB but missing in S3: " + missingInS3 + "\n");
                    }
                }

            } catch (Exception e) {
                context.getLogger().log("Error: " + e.getMessage() + "\n");
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("statusCode", 500);
                errorResponse.put("headers", Collections.singletonMap("Content-Type", "application/json"));
                errorResponse.put("body", "{\"message\": \"FAILED\"}");
                return errorResponse;

            }
            Map<String, Object> response = new HashMap<>();
            Map<String, String> headers = new HashMap<>();
            headers.put("Content-Type", "application/json");
            response.put("statusCode", 200);
            response.put("headers", headers);
            response.put("body", "{\"message\": \"Success\"}");
            return response;

        }

        private Set<String> listS3Objects(String bucketName) {
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .build();
            return s3.listObjectsV2(request).contents().stream()
                    .map(S3Object::key)
                    .collect(Collectors.toSet());
        }

        public List<ImageDto> fetchImages() throws Exception {
            List<ImageDto> images = new ArrayList<>();
            String sql = "SELECT id, name, image_size, file_extension, last_update FROM module9db.image";
            try (Statement stmt = dbConnection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    ImageDto dto = new ImageDto();
                    dto.setId(rs.getLong("id"));
                    dto.setName(rs.getString("name"));
                    dto.setImageSize(rs.getLong("image_size"));
                    dto.setFileExtension(rs.getString("file_extension"));
                    dto.setLastUpdate(rs.getTimestamp("last_update"));
                    images.add(dto);
                }
            }
            return images;
        }
    }