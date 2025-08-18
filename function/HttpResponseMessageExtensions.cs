using Microsoft.AspNetCore.Mvc;

namespace DLQReprocessing
{
    public static class HttpResponseMessageExtensions
    {
        public static async Task<ObjectResult> ToObjectResult(this HttpResponseMessage responseMessage)
        {
            if (responseMessage == null)
            {
                throw new ArgumentNullException(nameof(responseMessage));
            }

            var statusCode = (int)responseMessage.StatusCode;
            var content = await responseMessage.Content.ReadAsStringAsync();

            return new ObjectResult(content)
            {
                StatusCode = statusCode
            };
        }
    }
}