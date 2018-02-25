package oet.wouter.sandwich;

public class Location {

    private int id;
    private String street;

    public Location() {
    }

    public Location(int id, String street) {
        this.id = id;
        this.street = street;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }
}
