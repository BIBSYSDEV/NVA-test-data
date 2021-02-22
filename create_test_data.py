import users.import_authors
import users.import_users
import customers.import_customers
import publications.import_publications

customers.import_customers.run()
users.import_users.run()
users.import_authors.run()
publications.import_publications.run()
