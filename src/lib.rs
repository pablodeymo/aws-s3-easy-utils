use anyhow::anyhow;
use s3::{bucket::Bucket, creds::Credentials, region::Region, serde_types::Object};

pub fn get_s3_bucket(
    aws_access_key: &str,
    aws_secret_key: &str,
    aws_s3_region: &str,
    aws_s3_endpoint: &str,
    aws_s3_bucket: &str,
) -> Result<Bucket, anyhow::Error> {
    let creds = Credentials::new(Some(aws_access_key), Some(aws_secret_key), None, None, None)?;
    let region = Region::Custom {
        region: aws_s3_region.to_string(),
        endpoint: aws_s3_endpoint.to_string(),
    };

    Bucket::new(aws_s3_bucket, region, creds).map_err(|_e| anyhow!("Error getting bucket"))
}

pub async fn list_elements(bucket: &Bucket, prefix: &str) -> Result<Vec<Object>, anyhow::Error> {
    let mut ret: Vec<Object> = vec![];
    let mut list_search = bucket.list(prefix.to_string(), None).await?;
    for list in list_search.iter_mut() {
        ret.append(&mut list.contents);
    }
    Ok(ret)
}

pub struct AwsS3Info {
    pub aws_access_key: String,
    pub aws_secret_key: String,
    pub aws_s3_region: String,
    pub aws_s3_endpoint: String,
    pub aws_s3_bucket: String,
}

impl AwsS3Info {
    pub fn try_from_env() -> Result<Self, anyhow::Error> {
        let aws_access_key = std::env::var("AWS_ACCESS_KEY")?;
        let aws_secret_key = std::env::var("AWS_SECRET_KEY")?;
        let aws_s3_region = std::env::var("AWS_S3_REGION")?;
        let aws_s3_endpoint = std::env::var("AWS_S3_ENDPOINT")?;
        let aws_s3_bucket = std::env::var("AWS_S3_BUCKET")?;
        Ok(AwsS3Info {
            aws_access_key,
            aws_secret_key,
            aws_s3_region,
            aws_s3_endpoint,
            aws_s3_bucket,
        })
    }

    pub fn get_s3_bucket(&self) -> Result<Bucket, anyhow::Error> {
        get_s3_bucket(
            &self.aws_access_key,
            &self.aws_secret_key,
            &self.aws_s3_region,
            &self.aws_s3_endpoint,
            &self.aws_s3_bucket,
        )
    }
}
