import {Construct} from "constructs";
import {BlockPublicAccess, Bucket, BucketEncryption} from "aws-cdk-lib/aws-s3";
import {RemovalPolicy} from "aws-cdk-lib";

export interface S3ConstructProps {
    migrationBucketName: string;
}

export class S3Construct extends Construct {
    public readonly migrationBucket;

    constructor(scope: Construct, id: string, props: S3ConstructProps) {
        super(scope, id);

        const account = process.env["CDK_DEFAULT_ACCOUNT"];
        const region = process.env["CDK_DEFAULT_REGION"];
        let bucket_name = props.migrationBucketName || `solr2os-migration-${account}-${region}`

        this.migrationBucket = new Bucket(this, "Bucket", {
            bucketName: bucket_name,
            blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
            versioned: true,
            enforceSSL: true,
            encryption: BucketEncryption.S3_MANAGED,
            removalPolicy: RemovalPolicy.DESTROY
        });
    }

}