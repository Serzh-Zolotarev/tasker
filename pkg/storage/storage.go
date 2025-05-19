package storage

import (
	"context"

	"github.com/jackc/pgx/v4/pgxpool"
)

type Storage struct {
	db *pgxpool.Pool
}

func New(constr string) (*Storage, error) {
	db, err := pgxpool.Connect(context.Background(), constr)
	if err != nil {
		return nil, err
	}
	s := Storage{
		db: db,
	}
	return &s, nil
}

type Task struct {
	ID         int
	Opened     int64
	Closed     int64
	AuthorID   int
	AssignedID int
	Title      string
	Content    string
}

func (s *Storage) Tasks(taskID, authorID int) ([]Task, error) {
	rows, err := s.db.Query(context.Background(), `
		SELECT 
			id,
			opened,
			closed,
			author_id,
			assigned_id,
			title,
			content
		FROM tasks
		WHERE
			($1 = 0 OR id = $1) AND
			($2 = 0 OR author_id = $2)
		ORDER BY id;
	`,
		taskID,
		authorID,
	)
	if err != nil {
		return nil, err
	}

	var tasks []Task

	for rows.Next() {
		var t Task
		err = rows.Scan(
			&t.ID,
			&t.Opened,
			&t.Closed,
			&t.AuthorID,
			&t.AssignedID,
			&t.Title,
			&t.Content,
		)
		if err != nil {
			return nil, err
		}

		tasks = append(tasks, t)
	}

	return tasks, rows.Err()
}

func (s *Storage) TasksByAuthor(authorID int) ([]Task, error) {
	rows, err := s.db.Query(context.Background(), `
		SELECT 
			id,
			opened,
			closed,
			author_id,
			assigned_id,
			title,
			content
		FROM tasks
		WHERE
			($1 = 0 OR author_id = $1)
		ORDER BY id;
	`,
		authorID,
	)
	if err != nil {
		return nil, err
	}

	var tasks []Task

	for rows.Next() {
		var t Task
		err = rows.Scan(
			&t.ID,
			&t.Opened,
			&t.Closed,
			&t.AuthorID,
			&t.AssignedID,
			&t.Title,
			&t.Content,
		)
		if err != nil {
			return nil, err
		}

		tasks = append(tasks, t)
	}

	return tasks, rows.Err()
}

func (s *Storage) TasksByLabel(labelID int) ([]Task, error) {
	rows, err := s.db.Query(context.Background(), `
		SELECT 
			tasks.id,
			tasks.opened,
			tasks.closed,
			tasks.author_id,
			tasks.assigned_id,
			tasks.title,
			tasks.content,
			tasks_labels.label_id
		FROM tasks
		JOIN tasks_labels
		ON tasks.id = tasks_labels.task_id
		WHERE
			($1 = 0 OR label_id = $1)
		ORDER BY id;
	`,
		labelID,
	)
	if err != nil {
		return nil, err
	}

	var tasks []Task

	for rows.Next() {
		var t Task
		err = rows.Scan(
			&t.ID,
			&t.Opened,
			&t.Closed,
			&t.AuthorID,
			&t.AssignedID,
			&t.Title,
			&t.Content,
		)
		if err != nil {
			return nil, err
		}

		tasks = append(tasks, t)
	}

	return tasks, rows.Err()
}

func (s *Storage) NewTask(t Task) (int, error) {
	var id int
	err := s.db.QueryRow(context.Background(), `
		INSERT INTO tasks (title, content)
		VALUES ($1, $2) RETURNING id;
		`,
		t.Title,
		t.Content,
	).Scan(&id)

	return id, err
}

func (s *Storage) DeleteTask(id int) (int, error) {
	_, err := s.db.Exec(context.Background(), `
		DELETE FROM tasks (id)
		WHERE id = $1;
	`,
		id,
	)

	if err != nil {
		return id, err
	}

	return id, nil
}

func (s *Storage) UpdateTask(id int, task Task) (int, error) {
	_, err := s.db.Exec(context.Background(), `
		UPDATE tasks (author_id, assigned_id, title, content)
		SET author_id = $2, assigned_id = $3, title = $4, content = $5
		WHERE id = $1;
	`,
		id, task.AuthorID, task.AssignedID, task.Title, task.Content,
	)

	if err != nil {
		return id, err
	}

	return id, nil
}
