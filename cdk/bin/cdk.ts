#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import {Solr2OsStack} from "../lib/cdk-stack";

// Load environment variables from .env file
require('dotenv').config();

const app = new cdk.App();

// Get configuration from .env file or use defaults
const namePrefix = process.env.NAME_PREFIX || 'solr2os';
const domainName = process.env.DOMAIN_NAME;
const indexName = process.env.INDEX_NAME || 'solr-migration';
const cidr = process.env.VPC_CIDR || '10.0.0.0/16';
const enable_pipeline = process.env.ENABLE_PIPELINE === 'true';

const stack = new Solr2OsStack(app, "Solr2OS", {
    namePrefix,
    domainName,
    indexName,
    cidr,
    enable_pipeline: enable_pipeline
});