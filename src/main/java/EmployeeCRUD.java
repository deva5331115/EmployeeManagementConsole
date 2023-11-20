import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.InputMismatchException;
import java.util.Scanner;;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import com.mongodb.client.model.Filters;




public class EmployeeCRUD {

    private static Scanner scanner = new Scanner(System.in);
    private static MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
    private static MongoDatabase database = mongoClient.getDatabase("EmployeeTable");
    private static int empId;
    private static String empName;
    private static int age;
    private static String DOB;
    private static String DOJ;
    private static double salary;
    private static String department;

    public static void main(String[] args) {
        while (true) {
            try {
                System.out.println("Employee Management CRUD");
                System.out.println("1. Add Employee");
                System.out.println("2. Display All Employees");
                System.out.println("3. Filter");
                System.out.println("4. Search for an Employee");
                System.out.println("5. Update Employee");
                System.out.println("6. Delete Employee");
                System.out.println("7. Average salary of department");
                System.out.println("8. Average salary of specified department");
                System.out.println("9. Average salary of an Employee in the Company");
                System.out.println("10. Exit");
                System.out.print("Enter your choice: ");

                int choice = scanner.nextInt();


                switch (choice) {
                    case 1:
                        try {
                            addEmployee();
                        } catch (DuplicateKeyException e) {
                            System.out.println("Error: " + e.getMessage());
                        }
                        break;
                    case 2:
                        displayAllEmployees();
                        break;
                    case 3:
                        filter();
                        break;
                    case 4:
                        searchEmployee();

                        break;
                    case 5:
                        updateEmployee();

                        break;
                    case 6:
                        deleteEmployee();
                        break;
                    case 7:
                       avgsalDept();
                       break;
                    case 8:
                        avgspecDptSal();
                        break;
                    case 9:
                        avgOfAllEmployee();
                        break;
                    case 10:
                        System.out.println("Exiting the application. Goodbye!");
                        System.exit(0);
                        break;

                    default:
                        System.out.println("Invalid choice. Please try again.");
                }
            } catch (InputMismatchException e) {

                scanner.nextLine();
                System.out.println("Invalid input. Please enter a valid number.");
            }
        }
    }
    private static void addEmployee() {
        System.out.println("Enter Employee Details:");
        try {

            System.out.print("Employee ID: ");
            empId = scanner.nextInt();
            System.out.print("Employee Name: ");
            empName = scanner.next();
            System.out.print("Age: ");
            age = scanner.nextInt();
            System.out.print("Date of Birth (yyyy-MM-dd): ");
            DOB = scanner.next();
            LocalDate DOBB = parseDate(DOB);
            System.out.print("Date of Joining (yyyy-MM-dd): ");
            DOJ = scanner.next();
            LocalDate DOJJ = parseDate(DOJ);
            System.out.print("Salary: ");
            salary = scanner.nextDouble();
            System.out.print("Department: ");
            department = scanner.next();
            Document employeeDocument = new Document()
                    .append("_id", empId)
                    .append("empName", empName)
                    .append("Age", age)
                    .append("DOB", DOBB)
                    .append("DOJ", DOJJ)
                    .append("salary", salary)
                    .append("department", department);


            MongoCollection<Document> collection = database.getCollection("EmployeeDatas");
            try {
                collection.insertOne(employeeDocument);
                System.out.println("Employee added successfully to MongoDB!");
                System.out.println("-----------------------");
            } catch (com.mongodb.MongoWriteException e) {
                if (e.getError().getCode() == 11000) {
                    throw new DuplicateKeyException("Employee with the given ID already exists.", e);
                } else {
                    throw e;
                }
            }
        } catch (InputMismatchException e) {

            scanner.nextLine();


            System.out.println("Invalid input. Please enter a valid Details.");
        }


    }
    private static void displayAllEmployees() {

        MongoCollection<Document> collection = database.getCollection("EmployeeDatas");
        FindIterable<Document> documents = collection.find();
        if (!documents.iterator().hasNext()) {
            System.out.println("No employees found in MongoDB.");
        } else {
            System.out.println("Employee List from MongoDB:");
            try (MongoCursor<Document> cursor = documents.iterator()) {
                while (cursor.hasNext()) {
                    Document document = cursor.next();
                    int employeeId = document.getInteger("_id");
                    System.out.println("Employee ID: " + employeeId);
                    System.out.println("Employee Name: " + document.getString("empName"));
                    System.out.println("Employee Age: " + document.getInteger("Age"));
                    System.out.println("Employee DOB: " + document.getDate("DOB"));
                    System.out.println("Employee DOJ: " + document.getDate("DOJ"));
                    System.out.println("Salary: " + document.getDouble("salary"));
                    System.out.println("Department: " + document.getString("department"));
                    System.out.println("-----------------------");
                }
            }
        }
    }
    private static void filter() {
        System.out.println("Filter by");
        System.out.println("1. Name");
        System.out.println("2. Age");
        System.out.println("3. DOJ");
        System.out.println("4. Salary");
        System.out.println("5. Department");
        System.out.println("6. Multiple filter");
        System.out.print("Enter your choice: ");

        int choice = scanner.nextInt();
        switch (choice) {
            case 1:
                MongoCollection<Document> collection = database.getCollection("EmployeeDatas");
                System.out.print("Enter name");
                String prefix = scanner.next();
                Document filter = new Document("empName", new Document("$regex", "^" + prefix).append("$options", "i"));
                FindIterable<Document> documents = collection.find(filter);
                System.out.println("Employees with names starting with " + prefix + ":");
                try (MongoCursor<Document> cursor = documents.iterator()) {
                    while (cursor.hasNext()) {
                        Document document = cursor.next();
                        int employeeId = document.getInteger("_id");
                        System.out.println("Employee ID: " + employeeId);
                        System.out.println("Employee Name: " + document.getString("empName"));
                        System.out.println("Employee Name: " + document.getInteger("Age"));
                        System.out.println("Employee Name: " + document.getDate("DOB"));
                        System.out.println("Employee Name: " + document.getDate("DOJ"));
                        System.out.println("Salary: " + document.getDouble("salary"));
                        System.out.println("Department: " + document.getString("department"));
                        System.out.println("-----------------------");
                    }
                }


                break;
            case 2:
                MongoCollection<Document> collectionn = database.getCollection("EmployeeDatas");
                System.out.print("Enter Starting Age");
                int minAge = scanner.nextInt();
                System.out.print("Enter ending Age");
                int maxAge = scanner.nextInt();
                Document filters = new Document();
                filters.append("Age", new Document("$gte", minAge).append("$lte", maxAge));
                FindIterable<Document> documentss = collectionn.find(filters);
                System.out.println("Employees within age range " + minAge + " to " + maxAge + ":");
                try (MongoCursor<Document> cursor = documentss.iterator()) {
                    while (cursor.hasNext()) {
                        Document document = cursor.next();
                        int employeeId = document.getInteger("_id");
                        System.out.println("Employee ID: " + employeeId);
                        System.out.println("Employee Name: " + document.getString("empName"));
                        System.out.println("Employee Name: " + document.getInteger("Age"));
                        System.out.println("Employee Name: " + document.getDate("DOB"));
                        System.out.println("Employee Name: " + document.getDate("DOJ"));
                        System.out.println("Salary: " + document.getDouble("salary"));
                        System.out.println("Department: " + document.getString("department"));
                        System.out.println("-----------------------");
                    }
                }

                break;
            case 3:
                MongoCollection<Document> ollection = database.getCollection("EmployeeDatas");
                System.out.print("Enter Starting Date of joining");
                String st = scanner.next();
                System.out.print("Enter ending Date oj joining");
                String end = scanner.next();
                LocalDate startDate = parseDate(st);
                LocalDate endDate = parseDate(end);
                Bson filterss = Filters.and(
                        Filters.gte("DOJ", startDate),
                        Filters.lte("DOJ", endDate)
                );
                FindIterable<Document> documentsss = ollection.find(filterss);
                System.out.println("Employees joined between " + st + " and " + end + ":");
                try (MongoCursor<Document> cursor = documentsss.iterator()) {
                    while (cursor.hasNext()) {
                        Document document = cursor.next();
                        int employeeId = document.getInteger("_id");
                        System.out.println("Employee ID: " + employeeId);
                        System.out.println("Employee Name: " + document.getString("empName"));
                        System.out.println("Employee Name: " + document.getInteger("Age"));
                        System.out.println("Employee Name: " + document.getDate("DOB"));
                        System.out.println("Employee Name: " + document.getDate("DOJ"));
                        System.out.println("Salary: " + document.getDouble("salary"));
                        System.out.println("Department: " + document.getString("department"));
                    }
                }

                break;
            case 4:
                MongoCollection<Document> collectionnn = database.getCollection("EmployeeDatas");
                System.out.print("Enter Minimum Salary");
                Double minSalary = scanner.nextDouble();
                System.out.print("Enter Maximum Salary");
                Double maxSalary = scanner.nextDouble();
                Document filte = new Document();

                filte.append("Age", new Document("$gte", minSalary).append("$lte", maxSalary));
                FindIterable<Document> doc = collectionnn.find(filte);
                System.out.println("Employees within salary range $" + minSalary + " to $" + maxSalary + ":");
                try (MongoCursor<Document> cursor = doc.iterator()) {
                    while (cursor.hasNext()) {
                        Document document = cursor.next();
                        System.out.println("Employee ID: " + document.getInteger("_id"));
                        System.out.println("Employee Name: " + document.getString("empName"));
                        System.out.println("Salary: " + document.getDouble("salary"));
                        System.out.println("Department: " + document.getString("department"));
                        System.out.println("-----------------------");

                    }
                }
                break;
            case 5:
                MongoCollection<Document> collectionnnn = database.getCollection("EmployeeDatas");
                System.out.print("Enter Department");
                String depart = scanner.next();
                Document fil = new Document("department", depart);
                FindIterable<Document> docu = collectionnnn.find(fil);
                System.out.println("Employees in Department " + department + ":");
                try (MongoCursor<Document> cursor = docu.iterator()) {
                    while (cursor.hasNext()) {
                        Document document = cursor.next();
                        int employeeId = document.getInteger("_id");
                        System.out.println("Employee ID: " + employeeId);
                        System.out.println("Employee Name: " + document.getString("empName"));
                        System.out.println("Employee Age: " + document.getInteger("Age"));
                        System.out.println("Employee DOB: " + document.getDate("DOB"));
                        System.out.println("Employee DOJ: " + document.getDate("DOJ"));
                        System.out.println("Salary: " + document.getDouble("salary"));
                        System.out.println("Department: " + document.getString("department"));
                        System.out.println("-----------------------");
                    }
                }
                break;


            case 6:
                MongoCollection<Document> collectio = database.getCollection("EmployeeDatas");
                System.out.print("Enter Department");
                depart = scanner.next();
                System.out.print("Enter Starting Age");
                minAge = scanner.nextInt();
                System.out.print("Enter ending Age");
                maxAge = scanner.nextInt();
                System.out.print("Enter Minimum Salary");
                minSalary = scanner.nextDouble();
                System.out.print("Enter Maximum Salary");
                maxSalary = scanner.nextDouble();
                Document filtersss = new Document();
                filtersss.append("department", department);
                filtersss.append("Age", new Document("$gte", minAge).append("$lte", maxAge));
                filtersss.append("salary", new Document("$gte", minSalary).append("$lte", maxSalary));
                FindIterable<Document> documentst = collectio.find(filtersss);
                System.out.println("Employees meeting multiple conditions:");
                try (MongoCursor<Document> cursor = documentst.iterator()) {
                    while (cursor.hasNext()) {
                        Document document = cursor.next();
                        int employeeId = document.getInteger("_id");
                        System.out.println("Employee ID: " + employeeId);
                        System.out.println("Employee Name: " + document.getString("empName"));
                        System.out.println("Employee Age: " + document.getInteger("Age"));
                        System.out.println("Employee DOB: " + document.getDate("DOB"));
                        System.out.println("Employee DOJ: " + document.getDate("DOJ"));
                        System.out.println("Salary: " + document.getDouble("salary"));
                        System.out.println("Department: " + document.getString("department"));
                        System.out.println("-----------------------");
                    }
                }
                break;
            default:
                System.out.println("Invalid choice. Please try again.");
                filter();
        }
    }
    private static void searchEmployee() {
        MongoCollection<Document> collection = database.getCollection("EmployeeDatas");
        System.out.print("Enter name");
        String prefix = scanner.next();
        Document filter = new Document("empName", new Document("$regex", "^" + prefix).append("$options", "i"));
        FindIterable<Document> documents = collection.find(filter);
        System.out.println("Employees with names starting with " + prefix + ":");
        try (MongoCursor<Document> cursor = documents.iterator()) {
            while (cursor.hasNext()) {
                Document document = cursor.next();
                int employeeId = document.getInteger("_id");
                System.out.println("Employee ID: " + employeeId);
                System.out.println("Employee Name: " + document.getString("empName"));
                System.out.println("Employee Name: " + document.getInteger("Age"));
                System.out.println("Employee Name: " + document.getDate("DOB"));
                System.out.println("Employee Name: " + document.getDate("DOJ"));
                System.out.println("Salary: " + document.getDouble("salary"));
                System.out.println("Department: " + document.getString("department"));
                System.out.println("-----------------------");
            }
        }
    }
    private static void updateEmployee() {
        MongoCollection<Document> collection = database.getCollection("EmployeeDatas");
        System.out.print("Enter Employee ID to update: ");
        int employeeId = scanner.nextInt();
        Document existingEmployee = findEmployeeById(collection, employeeId);
        if (existingEmployee != null) {

            System.out.println("Enter which is to be updated");
            System.out.println("1. Employee Name");
            System.out.println("2. Age");
            System.out.println("3. DOB");
            System.out.println("4. DOJ");
            System.out.println("5. Salary");
            System.out.println("6. Department");
            System.out.println("7.Update multiple data");
            System.out.print("Enter your choice: ");

            int choice = scanner.nextInt();
            Document filter = new Document("_id", employeeId);


            switch (choice) {
                case 1:
                    System.out.println("Enter Employee Name");

                    String newEmpName = scanner.next();

                    Document updateOperation = new Document("$set", new Document("empName", newEmpName));
                    collection.updateOne(filter, updateOperation);
                    break;
                case 2:
                    System.out.println("Enter Employee Age");

                    int newEmpAge = scanner.nextInt();
                    Document updateOperation1 = new Document("$set", new Document("Age", newEmpAge));
                    collection.updateOne(filter, updateOperation1);
                    break;
                case 3:
                    System.out.println("Enter Employee DOB");
                    String newDOB = scanner.next();
                    LocalDate newDOBB = parseDate(newDOB);
                    Document updateOperation2 = new Document("$set", new Document("DOB", newDOBB));
                    collection.updateOne(filter, updateOperation2);
                    break;
                case 4:
                    System.out.println("Enter Employee DOJ");
                    String newDOJ = scanner.next();
                    LocalDate newDOJJ = parseDate(newDOJ);
                    Document updateOperation3 = new Document("$set", new Document("DOJ", newDOJJ));
                    collection.updateOne(filter, updateOperation3);
                    break;
                case 5:
                    System.out.println("Enter Employee Salary");
                    int newsalary = scanner.nextInt();
                    Document updateOperation4 = new Document("$set", new Document("salary", newsalary));
                    collection.updateOne(filter, updateOperation4);
                    break;
                case 6:
                    System.out.println("Enter Employee Department");
                    String newdept = scanner.next();
                    Document updateOperation5 = new Document("$set", new Document("department", newdept));
                    collection.updateOne(filter, updateOperation5);
                    break;
                case 7:
                    deleteEmployee();
                    addEmployee();
            }


        }
    }
    private static void deleteEmployee() {
        MongoCollection<Document> collection = database.getCollection("EmployeeDatas");


        System.out.print("Enter Employee ID to delete: ");
        int employeeId = scanner.nextInt();


        Document existingEmployee = findEmployeeById(collection, employeeId);

        if (existingEmployee != null) {
            Document filter = new Document("_id", employeeId);
            collection.deleteOne(filter);

            System.out.println("Employee record deleted successfully!");
        } else {
            System.out.println("Employee not found with ID: " + employeeId);
        }
    }
    private static void avgsalDept() {

        MongoCollection<Document> collection = database.getCollection("EmployeeDatas");
        AggregateIterable<Document> results = collection.aggregate(Arrays.asList(
                new Document("$group", new Document("_id", "$department")
                        .append("averageSalary", new Document("$avg", "$salary")))
        ));
        System.out.println("Average Salary by Department:");

        // Print the results in tabular format
        System.out.printf("%-20s %-20s%n", "Department", "Average Salary");
        System.out.println("----------------------------------------");
        for (Document result : results) {
            String department = result.getString("_id");
            double averageSalary = result.getDouble("averageSalary");

            System.out.printf("%-20s %-20.2f%n", department, averageSalary);
        }
    }
    private static void avgspecDptSal() {
        MongoCollection<Document> collection = database.getCollection("EmployeeDatas");
        System.out.print("Enter Department to calculate average salary: ");
        String departmentt = scanner.next();
        Double Sal;
        AggregateIterable<Document> result = collection.aggregate(Arrays.asList(
                new Document("$match", new Document("department", departmentt)),
                new Document("$group", new Document("_id", "$department").append("averageSalary", new Document("$avg", "$salary")))
        ));
        Document resultDocument = result.first();
        if (resultDocument != null) {
            Sal= resultDocument.getDouble("averageSalary");
        } else {
            Sal= 0.00;
        }
        System.out.println("Average Salary for Department " + department + ": " + Sal);

    }



    private static void avgOfAllEmployee() {
        MongoCollection<Document> collection = database.getCollection("EmployeeDatas");
        AggregateIterable<Document> result = collection.aggregate(Arrays.asList(
                new Document("$group", new Document("_id", null)
                        .append("averageSalary", new Document("$avg", "$salary")))
        ));
        double avgsalary;
        Document resultDocument = result.first();
        if (resultDocument != null) {
            avgsalary= resultDocument.getDouble("averageSalary");
        } else {
           avgsalary= 0.00;
        }
        System.out.println("Average Salary for All Employees: " + avgsalary);
    }



    private static Document findEmployeeById(MongoCollection<Document> collection, int employeeId) {

        Document filter = new Document("_id", employeeId);

        FindIterable<Document> documents = collection.find(filter);

        return documents.first();
    }


    private static LocalDate parseDate(String dateStr) {

        try {
            return LocalDate.parse(dateStr, DateTimeFormatter.ISO_DATE);
        } catch (DateTimeParseException e) {
            System.out.println("Invalid Date");
            return null;
        }
    }

   static class DuplicateKeyException extends RuntimeException {
        public DuplicateKeyException(String message, Throwable cause) {
            super(message, cause);
        }
    }

   static class DateParseException extends RuntimeException {
        public DateParseException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}




