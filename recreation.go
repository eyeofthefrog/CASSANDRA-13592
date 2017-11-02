package main

import (
	"bufio"
	"crypto/rsa"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"

	docker "github.com/fsouza/go-dockerclient"
	"github.com/gocql/gocql"
	"github.com/kelseyhightower/envconfig"
)

var conn *CassandraConnection
var dbConfig DBConfig
var privateKey *rsa.PrivateKey

type DBConfig struct {
	Keyspace string `default:"recreation"`
	Host     string `default:"127.0.0.1"`
	Port     string `default:""`
	Name     string `default:"recreation"`
	Timeout  int    `default:"30"`
}

type user struct {
	FirstName string
	LastName  string
	Age       int
}

func getConfig(app_name string, s interface{}) error {
	err := envconfig.Process(app_name, s)
	return err
}

func getPort() int {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func startCassandraDocker(name string, port string) error {
	var err error
	var container *docker.Container
	var authConfig docker.AuthConfiguration

	repo := "cassandra"
	tag := "3.11.1"
	endpoint := "unix:///var/run/docker.sock"
	client, _ := docker.NewClient(endpoint)

	exposedPort := map[docker.Port]struct{}{
		"9042/tcp": {},
	}

	createContConf := docker.Config{
		ExposedPorts: exposedPort,
		Image:        fmt.Sprintf("%s:%s", repo, tag),
		Env:          []string{"CASSANDRA_BROADCAST_ADDRESS=127.0.0.1"},
	}

	portBindings := map[docker.Port][]docker.PortBinding{
		"9042/tcp": {{HostIP: "", HostPort: port}},
	}

	createContHostConfig := docker.HostConfig{
		PortBindings:    portBindings,
		PublishAllPorts: false,
		Privileged:      false,
	}

	createContOps := docker.CreateContainerOptions{
		Name:       name,
		Config:     &createContConf,
		HostConfig: &createContHostConfig,
	}

	pullImageOps := docker.PullImageOptions{
		Repository: repo,
		Tag:        tag,
	}

	fmt.Println("Pulling Cassandra Docker image.")
	if err = client.PullImage(pullImageOps, authConfig); err != nil {
		return err
	}

	fmt.Println("Creating Cassandra container.")
	container, err = client.CreateContainer(createContOps)
	if err != nil {
		return err
	}

	fmt.Println("Starting Cassandra container.")
	if err = client.StartContainer(container.ID, nil); err != nil {
		return err
	}

	return nil
}

func destroyDockerContainer(containerID string) error {
	fmt.Println("Destroying Cassandra Docker container.")

	endpoint := "unix:///var/run/docker.sock"
	client, _ := docker.NewClient(endpoint)

	err := client.StopContainer(containerID, 30)

	err = client.RemoveContainer(docker.RemoveContainerOptions{
		ID:            containerID,
		RemoveVolumes: true,
		Force:         true,
	})

	return err
}

func waitForConnection(host string, port string, timeout int) error {
	fmt.Printf("Attempting to connect to DB %s:%s", host, port)
	iter := 0

	for iter < timeout {
		c := gocql.NewCluster(host)
		c.Timeout = 6 * time.Second
		c.Consistency = gocql.Quorum
		c.Keyspace = "system"
		c.Port, _ = strconv.Atoi(port)
		session, err := c.CreateSession()
		if err == nil {
			session.Close()
			break
		} else {
			fmt.Print(".")
			time.Sleep(1 * time.Second)
		}
	}
	fmt.Println("\nConnected to DB.")

	if iter >= timeout {
		return errors.New("timeout while waiting for the DB")
	}

	return nil
}

func setupDB(host string, port string, name string, keyspace string) (*CassandraConnection, error) {
	fmt.Println("Setting up the database.")

	var conn *CassandraConnection

	c := &CassandraConnection{}
	c.Hosts = []string{host}
	c.Port = port
	c.DBName = name
	c.Keyspace = keyspace
	conn = c

	if err := conn.Init(); err != nil {
		return nil, err
	}

	if err := conn.Connect(); err != nil {
		return nil, err
	}

	return conn, nil
}

func getContainer(name string) (*docker.APIContainers, error) {
	endpoint := "unix:///var/run/docker.sock"
	client, _ := docker.NewClient(endpoint)

	containers, err := client.ListContainers(docker.ListContainersOptions{All: true})
	if err != nil {
		return nil, err
	}

	for _, i := range containers {
		for _, n := range i.Names {
			if n == fmt.Sprintf("/%s", name) {
				return &i, nil
			}
		}
	}
	return nil, errors.New("no container found")
}

func tearDown() error {
	if dbConfig == (DBConfig{}) {
		err := getConfig("CASSANDRA_13592", &dbConfig)
		if err != nil {
			return err
		}
	}

	if conn != nil {
		conn.Close()
	}

	apiContainer, err := getContainer(dbConfig.Name)
	if err != nil {
		return err
	}

	if err = destroyDockerContainer(apiContainer.ID); err != nil {
		fmt.Printf("Unable to stop container: %s\n", err)
		return err
	}

	return nil
}

func scan(table string, pageSize int, pageState []byte) ([]user, []byte, error) {
	q := conn.Session.Query
	query := q(`SELECT first_name, last_name, age FROM users`)

	if pageSize > 0 {
		query.PageSize(10)
	}

	if pageState != nil {
		query.PageState(pageState)
	}

	iter := query.Iter()
	u := user{}
	users := make([]user, 0)
	for iter.Scan(&u.FirstName, &u.LastName, &u.Age) {
		users = append(users, u)

		if pageSize > 0 {
			if len(users) >= pageSize {
				break
			}
		}
	}
	next := iter.PageState()
	err := iter.Close()

	return users, next, err
}

func performRecreation() error {
	var err error
	var next []byte
	var users []user

	names := []string{"bill", "mary", "bob", "june", "peter", "sally",
		"rich", "patty", "henry", "nancy", "george", "allie"}

	q := conn.Session.Query

	for _, name := range names {
		if err = q(`INSERT INTO users (
			first_name, last_name, age)
			VALUES (?, ?, ?)`,
			name, "smith", rand.Intn(30)+20).Exec(); err != nil {
			return err
		}
	}

	//Query the whole table
	fmt.Println("Query all:")
	users, next, err = scan("users", 0, nil)
	if err != nil {
		return err
	}
	fmt.Println("\tResults:")
	for _, u := range users {
		fmt.Printf("\t\t%s %s, %d\n", u.FirstName, u.LastName, u.Age)
	}
	fmt.Println()

	//Query the first row, saving the state in "next"
	fmt.Println("Query one:")
	users, next, err = scan("users", 1, nil)
	if err != nil {
		return err
	}
	fmt.Println("\tResults:")
	for _, u := range users {
		fmt.Printf("\t\t%s %s, %d\n", u.FirstName, u.LastName, u.Age)
	}
	fmt.Println()

	//Query the whole table again.
	scan("users", 0, nil)

	//Query the next 5 using the state from the "Query one" query.
	fmt.Println("Query next 5:")
	users, next, err = scan("users", 5, next)

	if err != nil {
		if err.Error() == "java.lang.NullPointerException" {
			fmt.Printf("Got a NullPointerException. Run 'docker logs %s' in another window to see the stacktrace.\n", dbConfig.Name)
			fmt.Print("Press Enter to continue: ")
			bufio.NewReader(os.Stdin).ReadBytes('\n')
		} else {
			return err
		}
	}

	return nil
}

func main() {
	var err error

	err = getConfig("CASSANDRA_13592", &dbConfig)
	if err != nil {
		fmt.Printf("Error getting config values: %v\n", err)
		return
	}

	dbConfig.Port = strconv.Itoa(getPort())

	err = startCassandraDocker(dbConfig.Name, dbConfig.Port)
	if err != nil {
		fmt.Printf("Error starting container: %v\n", err)
		tearDown()
		return
	}

	err = waitForConnection(dbConfig.Host, dbConfig.Port, dbConfig.Timeout)
	if err != nil {
		fmt.Printf("Error waiting for connection: %v\n", err)
		tearDown()
		return
	}

	conn, err = setupDB(dbConfig.Host, dbConfig.Port, dbConfig.Name, dbConfig.Keyspace)
	if err != nil {
		fmt.Printf("Error setting up the DB: %v\n", err)
		tearDown()
		return
	}

	if err = performRecreation(); err != nil {
		fmt.Printf("Error performing recreation: %v\n", err)
		tearDown()
		return
	}

	tearDown()
}
