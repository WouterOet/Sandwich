package oet.wouter.sandwich;

public class SoldSandwich {

    private boolean partOfMenu;
    private String name;
    private int locationId;

    public SoldSandwich() {
    }

    public SoldSandwich(boolean partOfMenu, String name, int locationId) {
        this.partOfMenu = partOfMenu;
        this.name = name;
        this.locationId = locationId;
    }

    public boolean isPartOfMenu() {
        return partOfMenu;
    }

    public void setPartOfMenu(boolean partOfMenu) {
        this.partOfMenu = partOfMenu;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getLocationId() {
        return locationId;
    }

    public void setLocationId(int locationId) {
        this.locationId = locationId;
    }
}
