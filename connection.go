package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/gocql/gocql"
)

type CassandraConnection struct {
	User     string
	Password string
	Hosts    []string
	Port     string
	DBName   string
	Keyspace string
	Ca       string
	Cert     string
	Key      string
	Timeout  int
	Session  *gocql.Session
	Cluster  *gocql.ClusterConfig
}

func (cc *CassandraConnection) getCluster(keyspace string) *gocql.ClusterConfig {
	c := gocql.NewCluster(cc.Hosts...)
	c.Timeout = 60 * time.Second
	c.Consistency = gocql.Quorum
	c.Keyspace = keyspace
	c.Port, _ = strconv.Atoi(cc.Port)
	c.Authenticator = gocql.PasswordAuthenticator{
		Username: cc.User,
		Password: cc.Password,
	}

	if cc.Ca != "" && cc.Cert != "" && cc.Key != "" {
		c.SslOpts = &gocql.SslOptions{
			CaPath:                 cc.Ca,
			CertPath:               cc.Cert,
			KeyPath:                cc.Key,
			EnableHostVerification: false,
		}
	}

	return c
}

func (cc *CassandraConnection) getSession(keyspace string) (*gocql.Session, error) {
	c := cc.getCluster(keyspace)
	session, err := c.CreateSession()

	if err != nil {
		return nil, err
	}

	return session, nil
}

func (cc *CassandraConnection) createKeyspace(keyspace string) error {
	session, err := cc.getSession("system")

	if err != nil {
		return err
	}
	defer session.Close()

	qString := fmt.Sprintf("create keyspace if not exists %s with replication = { 'class' : '%s', 'replication_factor' : %d }",
		keyspace, "SimpleStrategy", 1)

	if err := session.Query(qString).Exec(); err != nil {
		return err
	}

	return nil
}

func (cc *CassandraConnection) Init() error {
	fmt.Println("Initializing the DB tables.")
	if err := cc.createKeyspace(cc.Keyspace); err != nil {
		return err
	}

	session, err := cc.getSession(cc.Keyspace)

	if err != nil {
		return err
	}

	defer session.Close()

	if err := session.Query(`
			create table if not exists users (
			first_name text, 
			last_name text,
			age int,
			primary key (first_name, last_name)
			)`).Exec(); err != nil {
		return err
	}

	fmt.Println("Initialization complete.")
	return err
}

func (cc *CassandraConnection) Connect() error {
	//TODO: Check to see if already connected
	c := cc.getCluster(cc.Keyspace)
	session, err := c.CreateSession()

	if err != nil {
		return err
	}

	cc.Cluster = c
	cc.Session = session

	return err
}

func (cc *CassandraConnection) Close() {
	if cc.Session != nil {
		cc.Session.Close()
	}
}
