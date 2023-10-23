namespace producer.Models
{
    public record Order(
        string Id,
        string Description,
        string Status,
        string Date,
        string Currency,
        double Amount,
        string CustomerId
    );
}
