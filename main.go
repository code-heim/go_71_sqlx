package main

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

var tables = `
CREATE TABLE authors (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	name TEXT NOT NULL,
	email TEXT UNIQUE NOT NULL
);

CREATE TABLE books (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	title TEXT NOT NULL,
	author_id INTEGER,
	published_year INTEGER,
	genre TEXT,
	FOREIGN KEY(author_id) REFERENCES authors(id)
);

CREATE TABLE members (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	name TEXT NOT NULL,
	email TEXT UNIQUE NOT NULL,
	join_date TEXT NOT NULL DEFAULT CURRENT_DATE
);
`

type Author struct {
	ID    int    `db:"id"`
	Name  string `db:"name"`
	Email string `db:"email"`
}

type Book struct {
	ID            int            `db:"id"`
	Title         string         `db:"title"`
	AuthorID      int            `db:"author_id"`
	PublishedYear int            `db:"published_year"`
	Genre         sql.NullString `db:"genre"`
}

type Member struct {
	ID       int    `db:"id"`
	Name     string `db:"name"`
	Email    string `db:"email"`
	JoinDate string `db:"join_date"`
}

func main() {
	// DB connection
	db, err := sqlx.Connect("sqlite3", "sqlx_demo.db")
	if err != nil {
		log.Fatalln(err)
	}

	// Create tables
	db.MustExec(tables)

	db.MustExec("INSERT INTO authors (name, email) VALUES ($1, $2)", "J.K. Rowling", "jk.rowling@codeheim.io")

	// Insert data using a transaction
	tx := db.MustBegin()
	tx.MustExec("INSERT INTO authors (name, email) VALUES ($1, $2)", "George R.R. Martin", "george.martin@codeheim.io")
	tx.MustExec("INSERT INTO books (title, author_id, published_year, genre) VALUES ($1, $2, $3, $4)", "Harry Potter", 1, 1997, "Fantasy")
	tx.MustExec("INSERT INTO books (title, author_id, published_year, genre) VALUES ($1, $2, $3, $4)", "Game of Thrones", 2, 1996, "Fantasy")
	tx.MustExec("INSERT INTO members (name, email) VALUES ($1, $2)", "John Doe", "john.doe@example.com")
	tx.Commit()

	// Query all authors
	var authors []Author
	err = db.Select(&authors, "SELECT * FROM authors")
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("Authors:", authors)

	fmt.Println("-------------------------------------------------")

	// Query a specific book by title
	var book Book
	err = db.Get(&book, "SELECT * FROM books WHERE title=$1", "Harry Potter")
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("Book Details:", book)

	fmt.Println("-------------------------------------------------")

	// Queries with Prepared Statements
	stmt, err := db.Preparex(`SELECT * FROM authors WHERE id=?`)
	if err != nil {
		log.Fatalln(err)
	}
	row := stmt.QueryRowx(1)
	var author Author
	err = row.StructScan(&author)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("Author from Prepared Query:", author)

	fmt.Println("-------------------------------------------------")

	// Query using IN clause
	var ids = []int{1, 2}
	query, args, err := sqlx.In("SELECT * FROM authors WHERE id IN (?);", ids)
	if err != nil {
		log.Fatalln(err)
	}
	query = db.Rebind(query)
	rows, err := db.Queryx(query, args...)
	if err != nil {
		log.Fatalln(err)
	}
	for rows.Next() {
		var author Author
		err := rows.StructScan(&author)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("Author from IN Clause: %+v\n", author)
	}

	fmt.Println("-------------------------------------------------")

	// Named Query with a Struct
	p := Book{AuthorID: 1}
	rows, err = db.NamedQuery(`SELECT * FROM books WHERE author_id=:author_id`, p)
	if err != nil {
		log.Fatalln(err)
	}
	for rows.Next() {
		var b Book
		err := rows.StructScan(&b)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("Book from Named Query (Struct): %+v\n", b)
	}

	fmt.Println("-------------------------------------------------")

	// Named Query with a Map
	m := map[string]interface{}{"name": "J.K. Rowling"}
	rows, err = db.NamedQuery(`SELECT * FROM authors WHERE name=:name`, m)
	if err != nil {
		log.Fatalln(err)
	}
	for rows.Next() {
		var a Author
		err := rows.StructScan(&a)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("Author from Named Query (Map): %+v\n", a)
	}

	fmt.Println("-------------------------------------------------")

	// Named Exec with a Map
	m = map[string]interface{}{"email": "new.email@example.com", "id": 1}
	result, err := db.NamedExec(`UPDATE authors SET email=:email WHERE id=:id`, m)
	if err != nil {
		log.Fatalln(err)
	}
	rowsAffected, _ := result.RowsAffected()
	fmt.Printf("Rows Updated: %d\n", rowsAffected)

	fmt.Println("-------------------------------------------------")

	// Insert a batch of new members
	members := []Member{
		{Name: "Alice", Email: "alice@example.com"},
		{Name: "Bob", Email: "bob@example.com"},
		{Name: "Charlie", Email: "charlie@example.com"},
	}

	_, err = db.NamedExec(`INSERT INTO members (name, email) VALUES (:name, :email)`,
		members)

	if err != nil {
		log.Fatalln(err)
	}

	// Query all members
	var allMembers []Member
	err = db.Select(&allMembers, "SELECT * FROM members ORDER BY join_date")

	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println("Members: ", allMembers)

	fmt.Println("-------------------------------------------------")

	// Delete a member by email
	result, err = db.Exec("DELETE FROM members WHERE email=$1", "john.doe@example.com")

	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println("Member deleted: ", result)
}
