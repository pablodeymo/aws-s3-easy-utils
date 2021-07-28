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
