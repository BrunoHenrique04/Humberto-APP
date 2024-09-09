using Microsoft.AspNetCore.Identity;

namespace HumbertoMVC.Models;

using System.Net.Http;
using System.Threading.Tasks;

public class ApiService
{
    private readonly HttpClient _httpClient;
    private bool _isAuthenticated = false;
    private string _authToken;

    public ApiService(HttpClient httpClient) // Recebe HttpClient via injeção de dependência
    {
        _httpClient = httpClient;
        _httpClient.BaseAddress = new Uri("http://api.olhovivo.sptrans.com.br");
        Console.WriteLine("----------------------------- EXECUTANDO API --------------------------------");
        
        /*
         * Eu não sei o porque mas o BaseAdress vai ignorar tudo que voce coloque após ".com.br"
         * então só vai funcionar se vocÊ fizer exatamente como está no codigo, talvez no futuro eu resolva isso
         * mas por enquanto coloque o "v2.1/" junto ao endpoint,
         * mesmo que a api do olho vivo esteja mandando colocar a versão junto ao endereço base, ignore.
         * A documentação não sabe o que está dizendo e não funciona de outra forma
         * ACREDITE EU JA TENTEI TODAS AS OUTRAS
         */
    }
    

    public async Task<bool> PostAuthenticateAsync(string token) //Autentica com o token
    {
        Console.WriteLine("---------------------------- Tentando Autenticar ---------------------------");
        
        try
        {
            //var response = await _httpClient.PostAsync($"/Login/Autenticar?token={token}", null);
            var response = await _httpClient.PostAsync($"v2.1/Login/Autenticar?token={token}", null);
            
            var responseContent = await response.Content.ReadAsStringAsync();
            bool isBodyTrue = responseContent.Contains("true", StringComparison.OrdinalIgnoreCase);

            if (isBodyTrue)
            {
                _isAuthenticated = true;
                Console.WriteLine("Retorno da autenticação na ApiServices: " + responseContent);
                return true; // Autenticação bem-sucedida
            }
            else
            {
                // Escreve a mensagem de erro no console
                Console.WriteLine($"*****Erro na autenticação: {response.StatusCode} - {responseContent}");
                Console.WriteLine("-----------------------------//------------------------------------");
                Console.WriteLine("URL AUTENTICATOR É: " + response.RequestMessage.RequestUri);
                Console.WriteLine("-----------------------------//------------------------------------");
                Console.WriteLine($"O retorno do body é: {isBodyTrue}");
                return false; // Falha na autenticação
            }
        }
        catch (Exception ex)
        {
            // Escreve a exceção no console
            Console.WriteLine($"Exceção durante a autenticação: {ex.Message}");
            return false; // Falha na autenticação devido a exceção
        }
    }
    
    
    
    public async Task<string> GetAsync(string endpoint, string token) //recebe o endpoint da api para getAsync
    {
        await _httpClient.PostAsync($"v2.1/Login/Autenticar?token={token}", null);
        var response = await _httpClient.GetAsync(endpoint);
        //response.EnsureSuccessStatusCode(); // Garante que o status da resposta foi 200-299

        return await response.Content.ReadAsStringAsync();
    }
    
    
}
