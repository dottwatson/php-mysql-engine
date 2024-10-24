<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Processor\QueryResult;
use Vimeo\MysqlEngine\Processor\ProcessorException;
use Vimeo\MysqlEngine\Query\Expression\BinaryOperatorExpression;
use Vimeo\MysqlEngine\Query\Expression\IntervalOperatorExpression;
use Vimeo\MysqlEngine\Query\Expression\RowExpression;
use Vimeo\MysqlEngine\Query\Expression\ConstantExpression;
use Vimeo\MysqlEngine\Query\Expression\FunctionExpression;
use Vimeo\MysqlEngine\Query\Expression\VariableExpression;
use Vimeo\MysqlEngine\Processor\Scope;
use Vimeo\MysqlEngine\Query\Expression\ColumnExpression;
use Vimeo\MysqlEngine\Schema\Column;
use Vimeo\MysqlEngine\TokenType;
use SimpleData\SimpleData;

final class JsonOperatorEvaluator{

    /**
     * @param array<string, mixed> $row
     * @param array<string, Column> $columns
     */
    public static function evaluate(
        \Vimeo\MysqlEngine\FakePdoInterface $conn,
        Scope $scope,
        BinaryOperatorExpression $expr,
        array $row,
        QueryResult $result
    ) {
        $right = $expr->right;
        $left = $expr->left;

        if( $left instanceof ColumnExpression){
            $l_value = Evaluator::evaluate($conn,$scope,$left,$row,$result);
            $l_value = json_decode($l_value);
        }
        elseif($left instanceOf ConstantExpression){
            $l_value = json_decode($left->value);
        }

        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new ProcessorException("Expected valid json string for {$expr->name} operand");
        }

        
        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new ProcessorException("Expected valid json string for {$expr->name} operand");
        }

        if ($right === null || !$right instanceof ConstantExpression) {
            throw new ProcessorException("Attempted to evaluate JsonOperatorExpression invalid rigth operand");
        }
        elseif(!static::validatePath($right->value)){
            throw new ProcessorException("Attempted to evaluate JsonOperatorExpression with invalid json path");
        }


        if(!static::validatePath($right->value)){
            throw new \Exception("Expected valid json path for {$expr->name} operand");
        }

        SimpleData::$PATH_SEPARATOR = '.';


        $document   = simple_data($l_value);
        $path       = JsonFunctionEvaluator::sanitizePathForSimpleData($right->value);
        $result     = $document->find($path);


        
        $endingResult =  ($result)
            ?$result->raw()
            :null;

        if(is_string($endingResult)){
            $endingResult = ($expr->operator == '->')
                ?json_encode($endingResult)
                :$endingResult;
        }
        else{
            $endingResult = json_encode($endingResult);
        }

        return $endingResult;
    }

    /**
     * Validates the JSON path expression.
     *
     * @param string $path The JSON path expression to validate.
     * @return bool True if valid, false otherwise.
     */
    public static function validatePath($path) {
        if($path == '$'){
            return true;
        }
        
        // Modified regex to validate JSON Path syntax with wildcards
        $pattern = '/^\$((\.(\w+|\*))|(\[\d+\])|(\[\*\])|(\.\*\*)|(\.\.))*$/';
        
        // Ensure the path doesn't contain '***'
        if (strpos($path, '***') !== false) {
            return false;
        }
        
        return preg_match($pattern, $path);
    }

    /**
     * check if json string is valid json or not
     *
     * @param string $jsonStr
     * @return bool
     */
    public static function validateJSON(string $jsonStr)
    {
        $test = json_decode($jsonStr);
        return (json_last_error() === JSON_ERROR_NONE);
    }
}
