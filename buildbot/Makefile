RIAK_CONF = ${RIAK_DIR}/etc/riak.conf
# ADVANCED_CONF = ${RIAK_DIR}/etc/advanced.config
# RIAK = ${RIAK_DIR}/bin/riak
RIAK_ADMIN = ${RIAK_DIR}/bin/riak-admin

preconfigure:
	@../setup.py preconfigure --riak-conf=${RIAK_CONF}

configure:
	@../setup.py configure --riak-admin=${RIAK_ADMIN}

compile:
	@../setup.py develop

lint:
	@cd ..; pep8 riak *.py
	@cd ..; pyflakes riak *.py

test:
	@echo "Testing Riak Python Client (without security)"
	@../setup.py test
