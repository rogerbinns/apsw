/*
    This file sets up a schema and content for demonstrating the
    Session extension.  The demonstration shows undo and redo,
    and merging changes made by different users independently.

    There is a table of items, a table of tags, and a table linking
    zero or more tags to each item.  Foreign keys and triggers are
    used to cause changes not by direct SQL (indirect changes in the
    extension).

    Rows in the items and tags tables need a unique id.  To allow
    changes to databases independent of each other, the usual approach
    is to use UUIDs which are random large numbers.  For this
    demonstration we use short random numbers to keep the output
    shorter and less cluttered.
*/

PRAGMA foreign_keys = ON;

-- individual tags
CREATE TABLE tags(
    id PRIMARY KEY DEFAULT (hex(randomblob(3))),
    label UNIQUE,
    cost_centre
);

-- individual items
CREATE TABLE items(
    id PRIMARY KEY DEFAULT (hex(randomblob(3))),
    name UNIQUE,
    description
);

-- maps a tag to an item.  there will be multiple rows
-- if an item has more than one tag.  if an item or
-- a tag are deleted, then the corresponding row
-- in this link table is automatically deleted
CREATE TABLE item_tag_link(
    item_id REFERENCES items(id) ON DELETE CASCADE,
    tag_id REFERENCES tags(id) ON DELETE CASCADE,
    reason,
    PRIMARY KEY(item_id, tag_id)
);

-- some example content
INSERT INTO tags(label, cost_centre) VALUES('new', 100);
INSERT INTO tags(label, cost_centre) VALUES('paint', 110);
INSERT INTO tags(label, cost_centre) VALUES('electrical', 145);
INSERT INTO tags(label, cost_centre) VALUES('inspection', 200);
INSERT INTO tags(label, cost_centre) VALUES('cleaning', 300);
INSERT INTO tags(label, cost_centre) VALUES('battery', 300);

INSERT INTO items(name) VALUES('bathroom ceiling');
INSERT INTO items(name, description) VALUES('bathroom lights', 'Four fixtures');
INSERT INTO items(name) VALUES('entrance floor');

-- link items and tags

INSERT INTO item_tag_link(item_id, tag_id)  VALUES(
    (SELECT id FROM items WHERE name='bathroom ceiling'),
    (SELECT id FROM tags WHERE label='paint')
);

INSERT INTO item_tag_link(item_id, tag_id)  VALUES(
    (SELECT id FROM items WHERE name='bathroom lights'),
    (SELECT id FROM tags WHERE label='electrical')
);

INSERT INTO item_tag_link(item_id, tag_id)  VALUES(
    (SELECT id FROM items WHERE name='bathroom lights'),
    (SELECT id FROM tags WHERE label='inspection')
);

INSERT INTO item_tag_link(item_id, tag_id)  VALUES(
    (SELECT id FROM items WHERE name='entrance floor'),
    (SELECT id FROM tags WHERE label='cleaning')
);

INSERT INTO item_tag_link(item_id, tag_id)  VALUES(
    (SELECT id FROM items WHERE name='entrance floor'),
    (SELECT id FROM tags WHERE label='battery')
);

-- any items added automatically have the 'new' tag
CREATE TRIGGER tag_new_items AFTER INSERT ON items
BEGIN
    INSERT INTO item_tag_link(item_id, tag_id, reason) VALUES(
        NEW.id,
        (SELECT id FROM tags WHERE label='new'),
        'system'
    );
END;

-- get an 'new' tag
INSERT INTO items(name) VALUES('entrance fan');
