### Environment Variables & Functionality Notes

This module is a wrapper for the Snowflake Python connector, using Django's native Postgres db connector as a model. It allows developers to use Django models and ORMs for CRUD operations.

The database wrapper is dependent upon a number of environment variables, which can be found under "Snowflake connection config" in settings.py.

To use this module, instructions for [certificate installation and proxy configuration](https://wiki.cglcloud.com/index.php/Proxy#CA_Bundles_required_for_proxy_connectivity_with_applications) should be followed.

In a development environment, insecure connections are enabled. This allows the Snowflake Python connector's SSL verification to be skipped. **While the above instructions allow for connectivity via Snowflake-CLI, connection issues were still observed with the Python connector on local machines**. Insecure connections should **not** be enabled for production.

The module was tested with the Cargill VPN disconnected.

### Tests

Currently this wrapper has one test, which validates the Snowflake connection.

Tests can be run in your local environment with the following command line argument:

```python cp_snowflake_api/manage.py test snowflake_wrapper.tests_snowflake_wrapper```