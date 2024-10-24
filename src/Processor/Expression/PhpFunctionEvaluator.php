<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\FakePdoInterface;
use Vimeo\MysqlEngine\Processor\ProcessorException;
use Vimeo\MysqlEngine\Processor\QueryResult;
use Vimeo\MysqlEngine\Processor\Scope;
use Vimeo\MysqlEngine\Query\Expression\ColumnExpression;
use Vimeo\MysqlEngine\Query\Expression\ConstantExpression;
use Vimeo\MysqlEngine\Query\Expression\Expression;
use Vimeo\MysqlEngine\Query\Expression\FunctionExpression;
use Vimeo\MysqlEngine\Query\Expression\IntervalOperatorExpression;
use Vimeo\MysqlEngine\Schema\Column;
use Vimeo\MysqlEngine\TokenType;

final class PhpFunctionEvaluator
{
    /**
     * @param array<string, mixed> $row
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    public static function evaluate(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) {
        switch ($expr->functionName) {
            case 'PHP_CALL':
                    return self::phpCall($conn, $scope, $expr, $row, $result);
            case 'PHP_EVAL':
                return self::phpEval($conn, $scope, $expr, $row, $result);
        }

        return new ProcessorException("Function " . $expr->functionName . " not implemented yet");
    }




    /**
     * Execute a callable function code, passing row values as parameters to the funciton.
     * You can use tables and columns values as parameters
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    protected static function phpCall($conn,$scope,$expr,$row,$result)
    {
        $params = $expr->args;

        if(count($params) == 0){
            throw new \Exception("PHP_CALL expects a callable argument. Can be a string, a json array or any other callable");
        }        

        /**
         * @var \Vimeo\MysqlEngine\Query\Expression\ConstantExpression
         */
        $callable = array_shift($params);
        $callable = $callable->value;
        $callback = null;

        // dd($callable,json_decode($callable),is_callable(json_decode($callable)));


        
        //check if json
        if(JsonOperatorEvaluator::validateJSON($callable)){
            $callback = json_decode($callable);
            // dd($callback);
            if(!is_callable($callback)){
                throw new \Exception("PHP_CALL expects a valid callable item to call. an invalid json is passed");
            }
        }
        else{
            // dd('here',$callable);
            if(!is_callable($callable)){
                throw new \Exception("PHP_CALL expects a valid callable item to call. an invalid string is passed");
            }

            $callback  = $callable;
        }

        // dd($callback);
        
        $params = array_map(function($item) use($conn,$scope,$expr,$row,$result) {
           return Evaluator::evaluate($conn,$scope,$item,$row,$result);
        },$params);

        
        $result = call_user_func_array($callback,$params);

        switch(gettype($result)){
            case 'boolean':
                return (int)$result;
            case 'array':
            case 'object':
                return json_encode($result);
            case 'integer':
            case 'double':
            case 'string':
                return $result;
            case 'NULL':
                return null;
            default:
                return gettype($result);
        }
    }


    /**
     * Execute a callable function code, passing row values as parameters to the funciton.
     * You can use tables and columns values as parameters
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    protected static function phpEval($conn,$scope,$expr,$row,$result)
    {
        $params = $expr->args;

        if(count($params) == 0){
            throw new \Exception("PHP_EVAL expects a code to execute.");
        }        

        $code = array_shift($params);
        $code = Evaluator::evaluate($conn,$scope,$code,$row,$result);

        try{
            $code = preg_replace([
                '#^<\?php#Usmi',
                '#?>$#Usmi',
            ],'',$code);

            $result = static::evalSandboxed($code);

            return static::makeResult($result);
        }
        catch(\Throwable $t){
            return (string)$t->getMessage();
        }

        //check if json
        if(JsonOperatorEvaluator::validateJSON($callable)){
            $callback = json_decode($callable);
            if(!is_callable($callable)){
                throw new \Exception("PHP_CALL expects a valid callable item to call. an invalid json is passed");
            }
        }
        else{
            if(!is_callable($callable)){
                throw new \Exception("PHP_CALL expects a valid callable item to call. an invalid string is passed");
            }

            $callback  = $callable;
        }

        
        $params = array_map(function($item) use($conn,$scope,$expr,$row,$result) {
           return Evaluator::evaluate($conn,$scope,$item,$row,$result);
        },$params);

        
        $result = call_user_func_array($callback,$params);

        return static::makeResult($result);
    }


    protected static function evalSandboxed($code)
    {
        return eval($code);
    }


    protected static function makeResult($value)
    {
        switch(gettype($value)){
            case 'boolean':
                return (int)$value;
            case 'array':
            case 'object':
                return json_encode($value);
            case 'integer':
            case 'double':
            case 'string':
                return $value;
            case 'NULL':
                return null;
            default:
                return gettype($value);
        }
    }

}
